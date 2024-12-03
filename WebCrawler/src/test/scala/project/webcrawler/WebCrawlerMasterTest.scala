package com.project.webcrawler.actors

// Reference: https://doc.akka.io/libraries/akka-core/current/testing.html

import akka.actor.{ActorSystem, Props}
import akka.testkit.{TestKit, TestProbe}
import com.project.webcrawler.{URLHeuristics, URLHeuristicsImpl}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import java.net.URL
import org.apache.spark.sql.SparkSession

class WebCrawlerMasterTest extends TestKit(ActorSystem("WebCrawlerMasterTest"))
  with AnyWordSpecLike
  with Matchers
  with BeforeAndAfterAll {

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "A WebCrawlerMaster" should {
    val spark = SparkSession.builder()
      .appName("Test")
      .master("local[*]")
      .getOrCreate()
    //Dont actually need anything here, just created it for testing purposes
    val mockHeuristics = new URLHeuristics {
      override def shouldBeIgnored(url: URL): Boolean = false
      override def getPriority(url: URL): Int = 0
    }
    "process Start message" in {
//      val spark = SparkSession.builder()
//        .appName("Test")
//        .master("local[*]")
//        .getOrCreate()
//      //Dont actually need anything here, just created it for testing purposes
//      val mockHeuristics = new URLHeuristics {
//        override def shouldBeIgnored(url: URL): Boolean = false
//        override def getPriority(url: URL): Int = 0
//      }

      val workerProbe = TestProbe()
      val master = system.actorOf(Props(new WebCrawlerMaster(spark, mockHeuristics) {
        override def preStart(): Unit = {createWorkers((2))}
        override def createWorkers(n: Int): Unit = {
          for (_ <- 1 to n) {
            println("creating worker")
            val worker = workerProbe.ref
            context.watch(worker)
            workers = worker :: workers
          }
        }
      }))

      val urlSeq= List(new URL("http://www1.coe.neu.edu/~rhillyard/indexSafe.html"))
      master ! Start(urlSeq)
      workerProbe.expectMsg(urlSeq.head)
    }

    "process Failed message" in {
      val workerProbe = TestProbe()

      val master = system.actorOf(Props(new WebCrawlerMaster(spark, mockHeuristics) {
        override def preStart(): Unit = createWorkers(1)

        override def createWorkers(n: Int): Unit = {
          val worker = workerProbe.ref
          context.watch(worker)
          workers = worker :: workers
        }
      }))
      val mockUrl = new URL("http://IAMMOCK.com")
      master ! Failed(mockUrl, new Exception("Test exception"))
    }

//    "process Done message" in {
//      val workerProbe = TestProbe()
//
//      val master = system.actorOf(Props(new WebCrawlerMaster(spark, mockHeuristics) {
//        override def preStart(): Unit = createWorkers(1)
//
//        override def createWorkers(n: Int): Unit = {
//          val worker = workerProbe.ref
//          context.watch(worker)
//          workers = worker :: workers
//        }
//      }))
//
//      val urls = List(new URL("http://www1.coe.neu.edu/~rhillyard/someUrl.html"))
//      val newVertices = Seq[(VertexId, String)]((1, "vertex1"))
//      val newEdges = Seq[Edge[String]](Edge(1, 2, "edge12"))
//
//      master ! Start(Nil)
//      workerProbe.expectMsgType[URL]  // Expecting the worker to receive a URL
//      master ! Done((urls, newVertices, newEdges))
//      workerProbe.expectMsgType[URL]  // Expecting the worker to receive another URL after Done
//
//      master ! GetQueueSize
//      expectMsg(1)
//    }
  }
}