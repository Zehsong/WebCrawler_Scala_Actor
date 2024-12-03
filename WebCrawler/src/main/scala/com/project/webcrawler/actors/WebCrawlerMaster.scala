// WebCrawlerMaster.scala
package com.project.webcrawler.actors

import akka.actor.{Actor, ActorRef, Props}
import com.project.webcrawler.{URLHeuristics, URLHeuristicsImpl}

import java.net.URL
import java.util.Comparator
import java.util.concurrent.PriorityBlockingQueue
import com.project.webcrawler.PrioritizedURL
import com.project.webcrawler.WebCrawler
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.sql.SparkSession
import scala.concurrent.duration._
import com.project.webcrawler.actors.{Start, Done, Failed, UnexpectedMessage, StopCrawl, GetQueueSize, GetEdgeCount, GetVertexCount}
import com.project.webcrawler.config.WebCrawlerConfig
import com.project.webcrawler.utils.GraphVisualizer
import com.project.webcrawler.utils.Neo4jConnector

class WebCrawlerMaster(spark: SparkSession, urlHeuristics: URLHeuristics) extends Actor {

  val webCrawler = WebCrawler(
    max = WebCrawlerConfig.maxDepth,
    parallelism = WebCrawlerConfig.parallelism,
    spark = spark
  )
  private var vertices: Seq[(VertexId, String)] = Seq.empty
  private var edges: Seq[Edge[String]] = Seq.empty
//  createWorkers(2)
override def preStart(): Unit = {
  super.preStart()
  println(s"Master preStart: ${self.path}")
  println("Initializing workers")
  createWorkers(WebCrawlerConfig.parallelism)
  println(s"Workers initialized: $workers")

  // Scheduling
  import context.dispatcher
  context.system.scheduler.scheduleOnce(WebCrawlerConfig.crawlTimeout, self, StopCrawl)
}

  override def postStop(): Unit = {
    println(s"Master postStop: ${self.path}")
    super.postStop()
  }

  private val inDegreeMap = scala.collection.mutable.HashMap[URL, Int]().withDefaultValue(0)
  // Avoid Duplication. Also I just realized I need page rank BRUHH, might just keep this here still
  private val visitedUrls = scala.collection.mutable.Set[URL]()
  private val urlComparator: Comparator[PrioritizedURL] =
    (url1: PrioritizedURL, url2: PrioritizedURL) => url1.priority.compare(url2.priority)
  private val queue = new PriorityBlockingQueue[PrioritizedURL](11, urlComparator)
  protected var workers = List.empty[ActorRef]

  def receive: Receive = {
    case Start(urls) =>
      println("start crawl send urls")
      urls.filterNot(urlHeuristics.shouldBeIgnored).foreach { url =>
        val priority = urlHeuristics.getPriority(url)
        queue.add(PrioritizedURL(url, priority))
      }
      sendWorkToFreeWorker()

    case Done(result) =>
      val (urls, newVertices, newEdges) = result
      println("Done Crawl Get More")
      urls.filterNot(urlHeuristics.shouldBeIgnored).foreach { url =>
        val priority = urlHeuristics.getPriority(url)
        queue.add(PrioritizedURL(url, priority))
        inDegreeMap(url) += 1
      }
      vertices = vertices ++ newVertices
      edges = edges ++ newEdges
      sendWorkToFreeWorker()

    case Failed(url, exception) =>
      println(s"Failed to process URL $url. Received failure message from worker ${sender().path.name}: $exception")

    case StopCrawl =>
      try {
        println("\n=== Web Crawler Summary ===")
        printPageRank()
        
        // Create graph and compute PageRank
        //println("\nComputing PageRank...")
        val verticesRDD = spark.sparkContext.parallelize(vertices)
        val edgesRDD = spark.sparkContext.parallelize(edges)
        val graph = Graph(verticesRDD, edgesRDD)
        val ranks = graph.pageRank(0.0001).vertices

        // Print graph statistics
        println("\nGraph Statistics:")
        println(s"Number of vertices: ${vertices.size}")
        println(s"Number of edges: ${edges.size}")

        // Populate Neo4j database

        println("\nPopulating Neo4j database...")
        Neo4jConnector.populateGraph(vertices, edges)

        
        println("\n=== Crawler Finished ===")
        
      } catch {
        case e: Exception => 
          println(s"Failed to create graph: ${e.getMessage}")
          println("Stack trace:")
          e.printStackTrace()
      } finally {

        try {
          println("\nClosing Neo4j connection...")
          Neo4jConnector.close()
          println("Neo4j connection closed successfully")
        } catch {
          case e: Exception =>
            println(s"Warning: Failed to close Neo4j connection: ${e.getMessage}")
        }
        // Stop everything after graph creation is complete
        println("\nShutting down workers and Spark...")
        workers.foreach(context.stop)
        context.parent ! akka.actor.PoisonPill
        context.stop(self)
        spark.stop()
      }

    case UnexpectedMessage(message) =>
      println(s"Received unexpected message from a worker: $message")

    // For Testing Purposes
    case GetQueueSize => sender() ! queue.size
    case GetVertexCount => sender() ! vertices.size
    case GetEdgeCount => sender() ! edges.size
  }

  def printPageRank(): Unit = {
    val sortedURLs = inDegreeMap.toSeq.sortBy(-_._2)
    sortedURLs.take(1).foreach { case (url, inDegree) =>
      println(s"URL: $url, In-degree: $inDegree")
    }
    println(s"Total crawl size: ${sortedURLs.size}")
  }

  private def sendWorkToFreeWorker(): Unit = {
    var continue = true
    while (continue) {
      Option(queue.poll()).foreach { prioritizedUrl =>
        if (!visitedUrls.contains(prioritizedUrl.url)) {
          visitedUrls.add(prioritizedUrl.url)
          getNextWorker() match {
            case Some(worker) =>
              println(s"Sending URL ${prioritizedUrl.url} to worker: ${worker.path}")
              worker ! prioritizedUrl.url
              continue = false
            case None =>
              println(s"No workers available to process URL: ${prioritizedUrl.url}")
              continue = false
          }
        } else {
          println(s"URL ${prioritizedUrl.url} has already been visited")
        }
      }
      if (queue.isEmpty) {
        continue = false
      }
    }
  }

  def queueSize: Int = queue.size

  def createWorkers(n: Int): Unit = {
    workers = (1 to n).map { _ =>
      val worker = context.actorOf(Props(classOf[WebCrawlerWorker], webCrawler))
      println(s"Worker created with path: ${worker.path}")
      worker
    }.toList
    println(s"Total workers created: ${workers.size}")
  }

  private def getNextWorker(): Option[ActorRef] = {
    println(s"Workers list before rotation: $workers")
    workers = workers match {
      case Nil =>
        println("No workers available in the list")
        Nil
      case head :: tail =>
        tail :+ head
    }
    println(s"Workers list after rotation: $workers")
    workers.headOption
  }
  def createGraph(): Unit = {
    try {
      println("Starting graph creation process...")

      // Create graph and compute PageRank
      println("Creating RDDs and computing PageRank...")
      val verticesRDD = spark.sparkContext.parallelize(vertices)
      val edgesRDD = spark.sparkContext.parallelize(edges)
      val graph = Graph(verticesRDD, edgesRDD)
      val ranks = graph.pageRank(0.0001).vertices

      // Print graph statistics
      println(s"\nGraph Statistics:")
      println(s"Number of vertices: ${vertices.size}")
      println(s"Number of edges: ${edges.size}")

      // Populate Neo4j database
      println("\nPopulating Neo4j database...")
      Neo4jConnector.populateGraph(vertices, edges)
      println("Neo4j population complete")
      println("\nYou can now view the graph in Neo4j Browser at http://localhost:7687")

    } catch {
      case e: Exception =>
        println(s"Failed to create graph: ${e.getMessage}")
        println("Stack trace:")
        e.printStackTrace()
    } finally {
      try {
        println("\nClosing Neo4j connection...")
        Neo4jConnector.close()
        println("Neo4j connection closed successfully")
      } catch {
        case e: Exception =>
          println(s"Warning: Failed to close Neo4j connection: ${e.getMessage}")
      }
    }
  }
}