// WebCrawlerWorker.scala
package com.project.webcrawler.actors

import akka.actor.Actor
import com.project.webcrawler.WebCrawler
import java.net.URL
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import org.apache.spark.graphx.{Edge, VertexId}
import com.project.webcrawler.actors.{Start, Done, Failed, UnexpectedMessage, StopCrawl, GetQueueSize, GetEdgeCount, GetVertexCount}


class WebCrawlerWorker(crawler: WebCrawler) extends Actor {
  def receive: Receive = {
    case url: URL =>
      println("WebCrawlerWorker received URL: "+url.toString+". Preparing WorkerDone message to reply to sender.")
      val senderRef = sender()
      val maxRetries = 3
      def attemptCrawl(retries: Int): Future[(Seq[URL], Seq[(VertexId, String)], Seq[Edge[String]])] = {
        crawler.crawl(List(url)).recoverWith {
          case ex if retries > 0 =>
            println(s"WebCrawlerWorker retrying $url: due to exception: "+ex.getMessage)
            attemptCrawl(retries - 1)
        }
      }
      attemptCrawl(maxRetries).onComplete {
        case scala.util.Success(result) =>
          println("WebCrawlerWorker sending WorkerDone message to sender.")
          senderRef ! Done(result)
        case scala.util.Failure(e) =>
          println("WebCrawlerWorker sending Failed(e) message to sender due to exception: "+e.getMessage)
          senderRef ! Failed(url, e)
      }
    case other =>
      val senderRefOther = sender()
      println(s"WebCrawlerWorker got an unexpected message: $other")
      senderRefOther ! UnexpectedMessage(other)
  }
}