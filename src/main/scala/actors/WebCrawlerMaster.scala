// WebCrawlerMaster.scala
package com.project.webcrawler.actors

import akka.actor.{Actor, ActorRef, Props}
import com.project.webcrawler.URLHeuristics.{getPriority, shouldBeIgnored}
import java.net.URL
import java.util.Comparator
import java.util.concurrent.PriorityBlockingQueue
import com.project.webcrawler.PrioritizedURL
import com.project.webcrawler.WebCrawler

case class Start(urls: List[URL])
case class Done(result: List[URL])

class WebCrawlerMaster extends Actor {

  val webCrawler = WebCrawler(20, 8)
  //  createWorkers(2)
  override def preStart(): Unit = {
    println("Initializing workers")
    createWorkers(2)
    println(s"Workers initialized: $workers")
  }

  private val inDegreeMap = scala.collection.mutable.HashMap[URL, Int]().withDefaultValue(0)
  // Avoid Duplication. Also I just realized I need page rank BRUHH, might just keep this here still
  private val visitedUrls = scala.collection.mutable.Set[URL]()
  private val urlComparator: Comparator[PrioritizedURL] =
    (url1: PrioritizedURL, url2: PrioritizedURL) => url1.priority.compare(url2.priority)
  private val queue = new PriorityBlockingQueue[PrioritizedURL](11, urlComparator)
  private var workers = List.empty[ActorRef]

  def receive: Receive = {
    case Start(urls) =>
      println("start crawl send urls")
      urls.filterNot(shouldBeIgnored).foreach { url =>
        val priority = getPriority(url)
        queue.add(PrioritizedURL(url, priority))
      }
      sendWorkToFreeWorker()

    case Done(result) =>
      println("Done Crawl Get More")
      result.filterNot(shouldBeIgnored).foreach { url =>
        val priority = getPriority(url)
        queue.add(PrioritizedURL(url, priority))
        inDegreeMap(url) += 1
      }
      sendWorkToFreeWorker()

    case url: URL =>
      if(!shouldBeIgnored(url)){
        val priority = getPriority(url)
        queue.add(PrioritizedURL(url, priority))
      }

    case Failed(e) =>
      println(s"Received failure message from a worker: $e")

    case _ =>
    // Other possible messages
  }

  private def sendWorkToFreeWorker(): Unit = {
    Option(queue.poll()).foreach { prioritizedUrl =>
      if (!visitedUrls.contains(prioritizedUrl.url)) {
        visitedUrls.add(prioritizedUrl.url)
        getNextWorker() match {
          case Some(worker) =>
            println(s"Sending URL ${prioritizedUrl.url} to worker: ${worker.path}")
            worker ! prioritizedUrl.url
          case None =>
            println(s"No workers available to process URL: ${prioritizedUrl.url}")
        }
      } else {
        println(s"URL ${prioritizedUrl.url} has already been visited")
      }
    }
  }


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

  def printPageRank(): Unit = {
    val sortedURLs = inDegreeMap.toSeq.sortBy(-_._2)
    sortedURLs.foreach { case (url, inDegree) =>
      println(s"URL: $url, In-degree: $inDegree")
    }
  }
}