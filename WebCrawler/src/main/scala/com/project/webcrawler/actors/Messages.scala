
package com.project.webcrawler.actors
import java.net.URL
import org.apache.spark.graphx.{Edge, VertexId}

case class Start(urls: List[URL])
case class Done(result: (Seq[URL], Seq[(VertexId, String)], Seq[Edge[String]]))
case class Failed(url: URL, e: Throwable)
case class UnexpectedMessage(message: Any)

case object StopCrawl
case object GetQueueSize
case object GetEdgeCount
case object GetVertexCount