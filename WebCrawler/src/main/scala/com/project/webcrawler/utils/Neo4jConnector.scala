package com.project.webcrawler.utils

import org.neo4j.driver.{AuthTokens, GraphDatabase, Session, Value, Values}
import org.apache.spark.graphx.{Edge, Graph}

object Neo4jConnector {
  private val uri = "bolt://localhost:7687"
  private val user = "neo4j"
  private val password = "passwordhere"
  
  private val driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password))

  def populateGraph(vertices: Seq[(Long, String)], edges: Seq[Edge[String]]): Unit = {
    val session = driver.session()
    try {
      println("Clearing existing data...")
      session.writeTransaction(tx => {
        tx.run("MATCH ()-[r]->() DELETE r")
        println("Cleared relationships")
        tx.run("MATCH (n) DELETE n")
        println("Cleared nodes")
      })

      println(s"\nCreating ${vertices.size} vertices...")
      val vertexBatchSize = 100
      vertices.grouped(vertexBatchSize).zipWithIndex.foreach { case (batch, batchIndex) =>
        session.writeTransaction(tx => {
          batch.foreach { case (id, url) =>
            val params = Values.parameters(
              "id", id.toString,
              "url", url
            )
            tx.run(
              "CREATE (p:Page {id: $id, url: $url})",
              params
            )
          }
          val progress = (batchIndex + 1) * vertexBatchSize min vertices.size
          println(s"Created vertices: $progress / ${vertices.size}")
        })
      }

      println(s"\nCreating ${edges.size} edges...")
      val edgeBatchSize = 100
      edges.grouped(edgeBatchSize).zipWithIndex.foreach { case (batch, batchIndex) =>
        session.writeTransaction(tx => {
          batch.foreach { edge =>
            val params = Values.parameters(
              "srcId", edge.srcId.toString,
              "dstId", edge.dstId.toString,
              "type", edge.attr
            )
            tx.run(
              """
              MATCH (source:Page {id: $srcId})
              MATCH (target:Page {id: $dstId})
              CREATE (source)-[r:LINKS_TO {type: $type}]->(target)
              """,
              params
            )
          }
          val progress = (batchIndex + 1) * edgeBatchSize min edges.size
          println(s"Created edges: $progress / ${edges.size}")
        })
      }

      session.writeTransaction(tx => {
        val nodeCount = tx.run("MATCH (n) RETURN count(n) as count").single().get("count").asLong()
        val edgeCount = tx.run("MATCH ()-[r]->() RETURN count(r) as count").single().get("count").asLong()
        println(s"\nFinal graph contains:")
        println(s"Nodes: $nodeCount")
        println(s"Edges: $edgeCount")
      })

    } catch {
      case e: Exception =>
        println(s"Failed to populate Neo4j database: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      session.close()
    }
  }

  def close(): Unit = {
    driver.close()
  }
} 