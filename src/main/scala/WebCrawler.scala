// WebCrawler.scala
package com.project.webcrawler

import com.project.webcrawler.WebCrawler.{canParse, wget}
import com.project.webcrawler.actors.WebCrawlerMaster
import com.project.webcrawler.actors.Start
import com.project.webcrawler.URLHeuristics.{getPriority, shouldBeIgnored}
import java.net.{MalformedURLException, URL}
import scala.collection.immutable.Queue
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

/**
 * Class to perform a web crawl.
 *
 * @param max the maximum number of recursive calls of the inner function in crawl.
 *            @param parallelism This is the number of elements we take from the queue each time in order to parallelize them.
 */
case class WebCrawler(max: Int, parallelism: Int = 8) {

  /**
   * Method to get all the URLs that can be crawled from any of the given URLs.
   *
   * @param us a list of URLs.
   * @return a Future of Seq[URL].
   */
  def crawl(us: Seq[URL])(implicit ec: ExecutionContext): Future[Seq[URL]] = {
    println(s"crawl: starting with $us")
    var vertices: Seq[(VertexId, String)] = Seq()
    var edges: Seq[Edge[String]] = Seq()
    val spark: SparkSession = SparkSession.builder
      .appName("WebCrawler")
      .master("local[*]")
      .getOrCreate()
    // NOTE: this is not tail-recursive since inner is invoked within the for comprehension.
    //    def inner(result: Seq[URL], queue: Queue[URL], m: Int): Future[Seq[URL]] = {
    //      val spark: SparkSession = SparkSession.builder
    //        .appName("WebCrawler")
    //        .master("local[*]")
    //        .getOrCreate()
    //
    //      if (result.nonEmpty) println(s"crawl: retrieved ${result.size} URLs:  $result")
    //      import com.project.webcrawler.QueueOps._
    //      m -> queue.dequeueN(parallelism) match {
    //        case (0, _) =>
    //          Future(result)
    //        case (_, (Nil, _)) =>
    //          Future(result)
    //        case (_, (urls, qq)) =>
    //          urls match {
    //            case Nil => Future(result)
    //            case xs =>
    //              for {
    //                us1 <- wget(xs)(logError)
    //                uq = qq.appendedAll(us1.filter(canParse)).distinct
    //                us2 = (result ++ xs).distinct
    //                us3 <- inner(us2, uq, m - 1)
    //              } yield {
    //                import spark.implicits._
    //                val df = us3.map(_.toString).toDF("url")
    //                df.show(false)
    //                us3
    //              }
    //          }
    //      }
    //    }
    def inner(result: Seq[(URL, URL)], queue: Queue[URL], m: Int): Future[Seq[(URL, URL)]] = {

      if (result.nonEmpty) println(s"crawl: retrieved ${result.size} URLs: $result")
      import com.project.webcrawler.QueueOps._
      m -> queue.dequeueN(parallelism) match {
        case (0, _) =>
          Future(result)
        case (_, (Nil, _)) =>
          Future(result)
        case (_, (us, uq)) =>
          for {
            us1 <- wget(us)(logError)
            uq1 = uq.appendedAll(us1.map(_._2)).distinct // Append the destination URLs
            us2 = (result ++ us1).distinct // Update the result
            us3 <- inner(us2, uq1, m - 1)
          } yield {
            import spark.implicits._
            val df = us3.toDF("src", "dst")
            df.show(false)
            val newVertices: RDD[(VertexId, String)] = spark.sparkContext.parallelize(
              (us3.map(_._1.toString) ++ us3.map(_._2.toString)).distinct.zipWithIndex.map{ case (url, index) => (index.toLong, url)}
            )
            val newEdges: RDD[Edge[String]] =
              spark.sparkContext.parallelize(
                us3.map { case (src, dst) => Edge(src.toString.toLong, dst.toString.toLong, src.toString + "->" + dst.toString) }
              )

            // Create a graph
            vertices = vertices ++ newVertices.collect()
            edges = edges ++ newEdges.collect()

            us3
          }
      }
    }
    //    inner(Nil, Queue().appendedAll(us), max)
    val finalVertices: RDD[(VertexId, String)] = spark.sparkContext.parallelize(vertices)
    val finalEdges: RDD[Edge[String]] = spark.sparkContext.parallelize(edges)
    val finalGraph = Graph(finalVertices, finalEdges)

    val flatFuture: Future[Seq[URL]] = inner(Nil.map(_ => (new URL(""), new URL(""))), Queue().appendedAll(us), max)
      .flatMap(seq => Future.successful(
        seq.flatMap{ case (srcUrl, destUrl) =>
          if (us.contains(destUrl)) Seq() else Seq(destUrl)
        }))
    flatFuture
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

  val urlSeq: Seq[String] = Seq("http://www1.coe.neu.edu/~rhillyard/indexSafe.html")
  //val urlsToCrawlAsString: Seq[String] = urlsToCrawl.map(_.toString)
  crawlWeb(urlSeq)

  /**
   * Main web-crawler method.
   *
   * @param ws the program arguments.
   */
  def crawlWeb(ws: Seq[String]): Unit = {
    MonadOps.sequence(ws map getURL) match {
      case Success(us) =>
        val urlsToCrawl = us
        val system = ActorSystem("CrawlerSystem")
        val master = system.actorOf(Props[WebCrawlerMaster], "master")
        master ! Start(urlsToCrawl.toList)
      case Failure(z) => println(s"Failure: $z")
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
   * @return a Future of Seq[(URL, URL)] which corresponds to the various A links in the HTML.
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