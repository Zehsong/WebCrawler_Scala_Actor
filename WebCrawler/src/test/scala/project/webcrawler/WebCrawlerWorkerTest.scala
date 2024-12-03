package project.webcrawler

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
import com.project.webcrawler.WebCrawler
import com.project.webcrawler.actors.{Failed, WebCrawlerWorker, Done}
import org.apache.spark.graphx.{Edge, VertexId}
import org.mockito.Mockito._
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import java.net.URL
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class WebCrawlerWorkerTest
  extends TestKit(ActorSystem("WebCrawlerWorkerTest"))
    with ImplicitSender
    with AnyFlatSpecLike
    with Matchers
    with BeforeAndAfterAll
    with MockitoSugar {

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  behavior of "WebCrawlerWorker"

  it should "send WorkerDone with result when crawl is successful" in {
    val crawler = mock[WebCrawler]
    val url = new URL("http://www1.coe.neu.edu/~rhillyard/indexSafe.html")
    val result = (Seq[URL](), Seq[(VertexId, String)](), Seq[Edge[String]]())
    when(crawler.crawl(List(url))).thenReturn(Future.successful(result))

    val worker = system.actorOf(Props(new WebCrawlerWorker(crawler)))
    worker ! url

    expectMsg(Done(result))
  }

  it should "send Failed when crawl throws an exception" in {
    val crawler = mock[WebCrawler]
    val url = new URL("http://test.com")
    val thrown = new Exception("Crawl failed")
    when(crawler.crawl(List(url))).thenReturn(Future.failed(thrown))

    val worker = system.actorOf(Props(new WebCrawlerWorker(crawler)))
    worker ! url

    expectMsg(Failed(url, thrown))
  }

  it should "handle unexpected messages gracefully" in {
    val crawler = mock[WebCrawler]
    val worker = system.actorOf(Props(new WebCrawlerWorker(crawler)))
    worker ! "unexpected"
  }
}