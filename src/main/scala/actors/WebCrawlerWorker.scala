// WebCrawlerWorker.scala
package com.project.webcrawler.actors

import akka.actor.Actor
import com.project.webcrawler.WebCrawler
import java.net.URL
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

case class Failed(e: Throwable)
case class Done(result: Seq[URL])

class WebCrawlerWorker(crawler: WebCrawler) extends Actor {
  def receive: Receive = {
    case url: URL =>
      val maxRetries = 3
      def attemptCrawl(retries: Int): Future[Seq[URL]] = {
        crawler.crawl(List(url)).recoverWith {
          case ex if retries > 0 =>
            println(s"Retrying $url: ${ex.getMessage}")
            attemptCrawl(retries - 1)
        }
      }
      attemptCrawl(maxRetries).onComplete {
        case scala.util.Success(result) => sender ! Done(result)
        case scala.util.Failure(e) => sender ! Failed(e)
      }
  }
}