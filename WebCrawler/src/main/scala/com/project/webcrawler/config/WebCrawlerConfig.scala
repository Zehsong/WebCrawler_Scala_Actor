package com.project.webcrawler.config

import com.typesafe.config.ConfigFactory
import scala.concurrent.duration._
import java.util.concurrent.TimeUnit

object WebCrawlerConfig {
  private val config = ConfigFactory.load()

  val sparkAppName: String = config.getString("spark.appName")
  val sparkMaster: String = config.getString("spark.master")

  val maxDepth: Int = config.getInt("webcrawler.maxDepth")
  val parallelism: Int = config.getInt("webcrawler.parallelism")

  // Convert Java Duration to Scala FiniteDuration
  val crawlTimeout: FiniteDuration = {
    val duration = config.getDuration("webcrawler.crawlTimeout")
    FiniteDuration(duration.toMillis, TimeUnit.MILLISECONDS)
  }
}