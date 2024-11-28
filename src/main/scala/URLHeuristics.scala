// URLHeuristics.scala

package com.project.webcrawler

import java.net.URL


case class PrioritizedURL(url: URL, priority: Int)
/**
 * Object to encapsulate the heuristic methods.
 */
object URLHeuristics {
  private val blackListedDomains: Set[String] = Set(
    "example.com",       // Replace with actual blacklisted domains.
    "example.org",       // Add more dark web or unwanted domains here.
    "malicious-site.net" // Example of a malicious domain.
  )

  private val highPriorityDomains: Set[String] = Set(
    "trusted.com",       // Replace with high-priority domains.
    "reliable.org"
  )

  /**
   * Heuristic to determine the priority of a URL.
   * Lower value indicates higher priority.
   *
   * @param url The URL to evaluate.
   * @return Priority value (lower is higher priority).
   */
  def getPriority(url: URL): Int = {
    val protocolPriority = if (url.getProtocol.toLowerCase == "https") 0 else 10
    val domainPriority = if (highPriorityDomains.contains(url.getHost)) -5 else 0
    val urlLengthPriority = math.min(url.toString.length / 100, 5) // Penalize very long URLs.
    val pathDepthPriority = url.getPath.count(_ == '/') // Deeper paths are deprioritized.

    protocolPriority + domainPriority + urlLengthPriority + pathDepthPriority
  }

  /**
   * Determines if a URL should be ignored.
   *
   * @param url The URL to evaluate.
   * @return True if the URL should be ignored, false otherwise.
   */
  def shouldBeIgnored(url: URL): Boolean = {
    // Check for blacklisted domains.
    if (blackListedDomains.contains(url.getHost)) {
      println(s"Blocked URL (blacklisted domain): ${url.toString}")
      return true
    }

    // Block URLs with certain file extensions (e.g., media files, executables).
    val blockedExtensions = Set("jpg", "png", "gif", "exe", "zip", "pdf")
    val extension = url.getPath.split('.').lastOption.getOrElse("").toLowerCase
    if (blockedExtensions.contains(extension)) {
      println(s"Blocked URL (unwanted extension): ${url.toString}")
      return true
    }

    // Block URLs with suspicious patterns (e.g., known malicious indicators).
    val suspiciousPatterns = List("login.php", "redirect", "track")
    if (suspiciousPatterns.exists(url.toString.contains)) {
      println(s"Blocked URL (suspicious pattern): ${url.toString}")
      return true
    }

// Allow the URL if it passes all checks.
    false
  }
}