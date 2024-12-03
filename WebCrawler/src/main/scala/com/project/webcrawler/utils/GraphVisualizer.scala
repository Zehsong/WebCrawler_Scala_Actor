package com.project.webcrawler.utils

import org.apache.spark.graphx.{Edge, Graph}
import guru.nidi.graphviz.engine.{Format, Graphviz, GraphvizEngine, GraphvizV8Engine}
import guru.nidi.graphviz.model.Factory._
import guru.nidi.graphviz.attribute.Color
import guru.nidi.graphviz.attribute.Style
import guru.nidi.graphviz.attribute.Label
import java.io.File
import guru.nidi.graphviz.model.MutableGraph
import guru.nidi.graphviz.model.MutableNode
import javax.swing.{JFrame, ImageIcon, JLabel, WindowConstants, SwingUtilities}
import java.awt.Dimension
import java.awt.image.BufferedImage

object GraphVisualizer {
  def visualizeGraph(vertices: Seq[(Long, String)], edges: Seq[Edge[String]], title: String = "Web Crawler Graph"): Unit = {
    println(s"Attempting to visualize graph with ${vertices.size} vertices and ${edges.size} edges")
    
    val g = mutGraph(title).setDirected(true)
    println("\nProcessing vertices:")
    vertices.foreach { case (id, url) =>
      println(s"ID: $id, URL: $url")
    }
    
    val vertexMap = vertices.toMap
    
    val nodes = vertices.map { case (id, url) =>
      val displayUrl = if (url.length > 30) url.take(27) + "..." else url
      println(s"Creating node for ID: $id, URL: $displayUrl")
      mutNode(id.toString).add(
        Style.FILLED,
        Color.rgb("#87CEEB"),
        Label.of(displayUrl)
      )
    }
    
    nodes.foreach(g.add)
    
    println("\nProcessing edges:")
    edges.foreach { edge =>
      val sourceUrl = vertexMap.getOrElse(edge.srcId, s"unknown-${edge.srcId}")
      val targetUrl = vertexMap.getOrElse(edge.dstId, s"unknown-${edge.dstId}")
      println(s"Adding edge: $sourceUrl -> $targetUrl")
      
      g.add(
        mutNode(edge.srcId.toString).addLink(
          mutNode(edge.dstId.toString)
        ).add(Label.of(edge.attr))
      )
    }
    
    try {
      println("\nGenerating visualization...")

      val currentDir = new File(".").getAbsoluteFile
      println(s"Current working directory: ${currentDir.getAbsolutePath}")

      val outputDir = new File("crawler_output")
      println(s"Attempting to create directory: ${outputDir.getAbsolutePath}")
      
      if (!outputDir.exists()) {
        val created = outputDir.mkdirs()
        println(s"Directory creation result: $created")
      } else {
        println("Output directory already exists")
      }

      val outputFile = new File(outputDir, "web_crawler_graph.png")
      println(s"Saving visualization to: ${outputFile.getAbsolutePath}")

      Graphviz.useEngine(new GraphvizV8Engine())
      
      Graphviz.fromGraph(g)
        .width(400)
        .height(300)
        .scale(0.5)
        .render(Format.PNG)
        .toFile(outputFile)
      
      println(s"Graph visualization saved to: ${outputFile.getAbsolutePath}")

      SwingUtilities.invokeLater(new Runnable {
        def run(): Unit = {
          try {
            val frame = new JFrame(title)
            frame.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE)
            
            val label = new JLabel(new ImageIcon(outputFile.getAbsolutePath))
            frame.getContentPane.add(label)
            
            frame.setMinimumSize(new Dimension(400, 300))
            frame.pack()
            frame.setLocationRelativeTo(null)
            frame.setVisible(true)
            frame.toFront()
            frame.repaint()
            
            println("Graph visualization window has been created and should be visible")
          } catch {
            case e: Exception => 
              println(s"Failed to display visualization window: ${e.getMessage}")
              e.printStackTrace()
          }
        }
      })
      
    } catch {
      case e: Exception => 
        println(s"Failed to generate graph visualization: ${e.getMessage}")
        e.printStackTrace()
    }
  }
}