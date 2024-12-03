// URLHeuristics.scala

package com.project.webcrawler

import java.net.URL


trait URLHeuristics {
  def getPriority(url: URL): Int
  def shouldBeIgnored(url: URL): Boolean
}

case class PrioritizedURL(url: URL, priority: Int)

class URLHeuristicsImpl extends URLHeuristics  {
  private val blackListedDomains: Set[String] = Set(
    "malicious",
    "porn",
    "hack",
    "onion"
  )

  /**
   * Heuristic to determine the priority of a URL.
   * Lower value indicates higher priority.
   *
   * @param url The URL to evaluate.
   * @return Priority value (lower is higher priority).
   */
  override def getPriority(url: URL): Int = {
    val protocolPriority = if (url.getProtocol.toLowerCase == "https") 0 else 10
    val urlLengthPriority = math.min(url.toString.length / 100, 5)
    val pathDepthPriority = url.getPath.count(_ == '/')

    protocolPriority + urlLengthPriority + pathDepthPriority
  }

  /**
   * Determines if a URL should be ignored.
   *
   * @param url The URL to evaluate.
   * @return True if the URL should be ignored, false otherwise.
   */
  override def shouldBeIgnored(url: URL): Boolean = {

    val validProtocols = Set("http", "https")

    if (!validProtocols.contains(url.getProtocol)) {
      println(s"Blocked URL (unsupported protocol): ${url.toString}")
      return true
    }

    if (blackListedDomains.exists(domain => url.getHost.contains(domain))) {
      println(s"Blocked URL (blacklisted domain): ${url.toString}")
      return true
    }

    if (url.getProtocol == "javascript") {
      println(s"Blocked URL (JavaScript link): ${url.toString}")
      return true
    }

    val blockedExtensions = Set("jpg", "png", "gif", "exe", "zip", "pdf")
    val extension = url.getPath.split('.').lastOption.getOrElse("").toLowerCase
    if (blockedExtensions.contains(extension)) {
      println(s"Blocked URL (unwanted extension): ${url.toString}")
      return true
    }
    false
  }
}