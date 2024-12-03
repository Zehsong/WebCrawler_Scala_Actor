// WebCrawler.scala
package com.project.webcrawler

import com.project.webcrawler.WebCrawler.{canParse, wget}
import com.project.webcrawler.actors.WebCrawlerMaster
import com.project.webcrawler.actors.Start
import java.net.{MalformedURLException, URL}
import scala.concurrent._
import scala.concurrent.duration._
import scala.io.{BufferedSource, Source}
import scala.language.postfixOps
import scala.util._
import scala.util.control.NonFatal
import scala.util.matching.Regex
import scala.xml.Node
import akka.actor.{ActorSystem, Props}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import com.project.webcrawler.actors.{StopCrawl}
import com.project.webcrawler.config.WebCrawlerConfig

/**
 * Class to perform a web crawl.
 *
 * @param max the maximum number of recursive calls of the inner function in crawl.
 *            @param parallelism This is the number of elements we take from the queue each time in order to parallelize them.
 */
case class WebCrawler(max: Int, parallelism: Int = 8, spark: SparkSession) {



  def crawl(us: Seq[URL])(implicit ec: ExecutionContext): Future[(Seq[URL], Seq[(VertexId, String)], Seq[Edge[String]])] = {
    println(s"crawl: starting with $us")

    // Validation for a valid url
    val validUrls = us.filter(canParse)
    if (validUrls.isEmpty) {
      return Future.successful((Seq.empty[URL], Seq.empty[(VertexId, String)], Seq.empty[Edge[String]]))
    }
    var vertices: Seq[(VertexId, String)] = Seq()
    var edges: Seq[Edge[String]] = Seq()

    wget(us)(logError).map { retrievedUrls =>
      val newUrls = retrievedUrls.map(_._2)
      val urlVertices = newUrls.zipWithIndex.map { case (url, id) => (id.toLong, url.toString) }

      val urlToId = urlVertices.map { case (id, url) => (url, id) }.toMap
      val urlEdges = retrievedUrls.flatMap { case (src, dst) => 
        for {
          srcId <- Option(urlToId.getOrElse(src.toString, -1L))
          dstId <- Option(urlToId.getOrElse(dst.toString, -1L))
          if srcId >= 0 && dstId >= 0
        } yield Edge(srcId, dstId, "link")
      }

      vertices = vertices ++ urlVertices
      edges = edges ++ urlEdges
      println(s"Number of URLs crawled: ${newUrls.size}")
      (newUrls, vertices, edges)
    }.recover {
      case ex =>
        logError(ex)
        (Seq.empty[URL], Seq.empty[(VertexId, String)], Seq.empty[Edge[String]])
    }
  }


  def logError(x: Throwable): Unit = System.err.println(s"""crawl: ignoring exception $x ${if (x.getCause != null) " with cause " + x.getCause else ""}""")
}


/**
 * Web Crawler App.
 *
 * @author scalaprof
 */
object WebCrawler extends App {

  /**
   * The "main" Web Crawler program.
   */

//  val urlsToCrawl = List(
//    new URL("https://crawler-test.com/"),
//    new URL("https://webmasters.stackexchange.com/questions/42720/is-there-any-seo-risk-of-using-javascript-instead-of-real-links")
//  )
  def createSparkSession(): SparkSession = {
    SparkSession.builder.appName(WebCrawlerConfig.sparkAppName).master(WebCrawlerConfig.sparkMaster).getOrCreate()
  }
    val urlSeq: Seq[String] = Seq("http://www1.coe.neu.edu/~rhillyard/indexSafe.html", "https://ml.cs.tsinghua.edu.cn/~jun/index.shtml")
  val urlHeuristics: URLHeuristics = new URLHeuristicsImpl()
  //val urlsToCrawlAsString: Seq[String] = urlsToCrawl.map(_.toString)
  crawlWeb(urlSeq, urlHeuristics)


  def crawlWeb(ws: Seq[String], urlHeuristics: URLHeuristics): Unit = {
    MonadOps.sequence(ws map getURL) match {
      case Success(us) =>
        val urlsToCrawl = us
        val system = ActorSystem("CrawlerSystem")

        // Add shutdown hook
        sys.addShutdownHook {
          println("Shutting down actor system...")
          // Tell master to stop crawling
          Option(system.actorSelection("/user/master")).foreach(_ ! StopCrawl)

          // Wait for termination with timeout
          try {
            // Wait up to 30 seconds for the actor system to shutdown
            Await.result(system.terminate(), 30.seconds)
            println("Actor system terminated successfully")
          } catch {
            case e: TimeoutException =>
              println("Actor system termination timed out")
              system.terminate()
          }
        }

        val sparkSession = createSparkSession()
        val master = system.actorOf(Props(new WebCrawlerMaster(sparkSession, urlHeuristics)), "master")
        master ! Start(urlsToCrawl.toList)

      case Failure(z) =>
        println(s"Failure: $z")
    }
  }

  /**
   * Method to read the content of the given URL and return the result as a Future[String].
   *
   * @param u a URL.
   * @return a String wrapped in Future.
   */
  def getURLContent(u: URL)(implicit ec: ExecutionContext): Future[String] =
    for {
      s <- MonadOps.asFuture(SourceFromURL(u))
      w <- MonadOps.asFuture(sourceToString(s, s"Cannot read from source at $u"))
    } yield w

  /**
   * Method to validate a URL as using either HTTPS or HTTP protocol.
   *
   * @param u a URL.
   * @return a Try[URL] which is a Success only if protocol is valid.
   */
  def validateURL(u: URL) = u.getProtocol match {
    case "https" | "http" => Success(u)
    case p => Failure(WebCrawlerProtocolException(p))
  }

  /**
   * Method to try to predict if a URL can be read as an HTML document.
   *
   * @param u a URL.
   * @return true if the extension (or lack thereof) is appropriate.
   */
  def canParse(u: URL): Boolean = {
    val fileNameExtension: Regex = """^([\/-_\w~]*\/)?([-_\w]*)?(\.(\w*))?$""".r
    u.getPath match {
      case fileNameExtension(_, _, _, null) => true
      case fileNameExtension(_, _, _, ext) => ext match {
        case "html" | "htm" => true
        case _ => false
      }
      case _ => true
    }
  }

  /**
   * Method to get a list of URLs referenced by the given URL.
   *
   * @param url a URL.
   * @return a Future of Seq[URL] which corresponds to the various A links in the HTML.
   */
//  def wget(url: URL)(implicit ec: ExecutionContext): Future[Seq[URL]] = {
//    // Hint: write as two nested for-comprehensions: the outer one (first) based on Seq, the inner (second) based on Try.
//    // In the latter, use the method createURL(Option[URL], String) to get the appropriate URL for a relative link.
//    // Don't forget to run it through validateURL.
//    def getURLs(ns: Node): Seq[Try[URL]] = {
//      for {
//        href <- (ns \\ "a").flatMap(node => node.attribute("href")).map(_.text).toList
//      } yield {
//        Try {
//          val absoluteUrl = if (href.startsWith("http")) {
//            new URL(href)
//          } else {
//            new URL(url, href)
//          }
//          validateURL(absoluteUrl).get
//        }
//      }
//    }
//
//    def getLinks(g: String): Try[Seq[URL]] = {
//      val ny: Try[Node] = HTMLParser.parse(g) recoverWith { case f => Failure(new RuntimeException(s"parse problem with URL $url: $f")) }
//      for (n <- ny; uys = getURLs(n); us <- MonadOps.sequenceForgiveSubsequent(uys) { case _: WebCrawlerProtocolException => true; case _ => false }) yield us
//    }
//    // Hint: write as a for-comprehension, using getURLContent (above) and getLinks above. You will also need MonadOps.scala.asFuture
//
//    for {
//      content <- getURLContent(url)
//      linksTry = getLinks(content)
//      links <- MonadOps.asFuture(linksTry)
//    } yield links
//  }

  /**
   * Method to get a list of URLs referenced by the given URL, along with the source URL.
   *
   * @param url a URL.
   * @return a Future of Seq[(URL, URL)] which corresponds to the various links in the HTML.
   */
  def wget(url: URL)(implicit ec: ExecutionContext): Future[Seq[(URL, URL)]] = {
    // Code truncated for brevity

    def getURLs(ns: Node): Seq[Try[(URL, URL)]] = {
      for {
        href <- (ns \\ "a").flatMap(node => node.attribute("href")).map(_.text).toList
      } yield {
        Try {
          val absoluteUrl = if (href.startsWith("http")) {
            new URL(href)
          } else {
            new URL(url, href)
          }
          (url, validateURL(absoluteUrl).get)
        }
      }
    }

    def getLinks(g: String): Try[Seq[(URL, URL)]] = {
      val ny: Try[Node] = HTMLParser.parse(g) recoverWith { case f => Failure(new RuntimeException(s"parse problem with URL $url: $f")) }
      for (n <- ny; uys = getURLs(n); us <- MonadOps.sequenceForgiveSubsequent(uys) { case _: WebCrawlerProtocolException => true; case _ => false }) yield us
    }

    for {
      content <- getURLContent(url)
      linksTry = getLinks(content)
      links <- MonadOps.asFuture(linksTry)
    } yield links
  }

  def wget(us: Seq[URL])(f: Throwable => Unit)(implicit ec: ExecutionContext): Future[Seq[(URL, URL)]] = {
    val usfs: Seq[Future[Seq[(URL, URL)]]] = for (u <- us) yield wget(u)
    val flattened: Future[Seq[(URL, URL)]] = Future.sequence(usfs).map(_.flatten)
    flattened.recover {
      case NonFatal(x) => f(x); Seq.empty[(URL, URL)]
      case x => f(x); Seq.empty[(URL, URL)]
    }
  }

  /**
   * For a given list of URLs, get a (flattened) sequence of URLs by invoking wget(URL).
   *
   * @param us a list of URLs.
   * @param f a function to log an exception (which will be ignored, provided that it is non-fatal).
   * @return a Future of Seq[URL].
   */
//  def wget(us: Seq[URL])(f: Throwable => Unit)(implicit ec: ExecutionContext): Future[Seq[URL]] = {
//    // CONSIDER using flatten of Seq Future Seq
//    val usfs: Seq[Future[Seq[URL]]] = for (u <- us) yield wget(u)
//    val usyf: Future[Try[Seq[URL]]] = for {
//      usys <- MonadOps.sequenceImpatient(usfs)(5000)
//      q = MonadOps.sequenceForgivingWith(usys) {
//        case NonFatal(x) => f(x); Success(None)
//        case x => Failure(x)
//      }
//    } yield for (qq <- q) yield qq.flatten
//    MonadOps.flatten(usyf)
//  }

  private def sourceToString(source: BufferedSource, errorMsg: String): Try[String] =
    try Success(source mkString) catch {
      case NonFatal(e) => Failure(WebCrawlerURLException(errorMsg, e))
    }

  private def getURL(resource: String): Try[URL] = createURL(None, resource)

  private def createURL(context: Option[URL], resource: String): Try[URL] =
    try Success(new URL(context.orNull, resource)) catch {
      case e: MalformedURLException => Failure(WebCrawlerURLException(context.map(_.toString).getOrElse("") + s"$resource", e))
      case NonFatal(e) => Failure(WebCrawlerException(context.map(_.toString).getOrElse("") + s"$resource", e))
    }

  private def SourceFromURL(resource: URL): Try[BufferedSource] = Try(Source.fromURL(resource))
}

case class WebCrawlerURLException(url: String, cause: Throwable) extends Exception(s"Web Crawler could not decode URL: $url", cause)

case class WebCrawlerProtocolException(url: String) extends Exception(s"Web Crawler does not support protocol: $url")

case class WebCrawlerException(msg: String, cause: Throwable) extends Exception(msg, cause)

object WebCrawlerException {
  def apply(msg: String): WebCrawlerException = apply(msg, null)
}

case class Unstring(n: Int) extends CharSequence {
  def +(s: String): String = s.substring(n)

  def length(): Int = -n

  def charAt(index: Int): Char = throw UnstringException(s"charAt: $index")

  def subSequence(start: Int, end: Int): CharSequence = throw UnstringException(s"subSequence: $start, $end")
}

case class UnstringException(str: String) extends Exception(str)