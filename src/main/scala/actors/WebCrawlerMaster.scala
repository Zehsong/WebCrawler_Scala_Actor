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
  createWorkers(10)

  private val inDegreeMap = scala.collection.mutable.HashMap[URL, Int]().withDefaultValue(0)

  private val visitedUrls = scala.collection.mutable.Set[URL]() // Avoid Duplication
  private val urlComparator: Comparator[PrioritizedURL] =
    (url1: PrioritizedURL, url2: PrioritizedURL) => url1.priority.compare(url2.priority)
  private val queue = new PriorityBlockingQueue[PrioritizedURL](11, urlComparator)
  private var workers = List.empty[ActorRef]

  def receive: Receive = {
    case Start(urls) =>
      urls.filterNot(shouldBeIgnored).foreach { url =>
        val priority = getPriority(url)
        queue.add(PrioritizedURL(url, priority))
      }
      sendWorkToFreeWorker()

    case Done(result) =>
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
        val worker = getNextWorker()
        worker ! prioritizedUrl.url
      }
    }
  }

  private def getNextWorker(): ActorRef = {
    // Rotate list of workers so load is distributed evenly across all workers.
    workers = workers match {
      case head :: tail => tail :+ head
      case _ => Nil
    }
    // Return first worker in rotated list.
    workers.head
  }

  // Method to initialize worker actors.
  def createWorkers(n: Int): Unit = {
    workers = (1 to n).map { _ =>
      context.actorOf(Props(classOf[WebCrawlerWorker], webCrawler))
    }.toList
  }

  def printPageRank(): Unit = {
    val sortedURLs = inDegreeMap.toSeq.sortBy(-_._2) // Sorting in descending order of in-degree
    sortedURLs.foreach { case (url, inDegree) =>
      println(s"URL: $url, In-degree: $inDegree")
    }
  }
}