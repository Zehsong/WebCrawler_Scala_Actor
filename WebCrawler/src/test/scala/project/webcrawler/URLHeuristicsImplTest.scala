package com.project.webcrawler.actors

import org.scalatest.flatspec.AnyFlatSpec
import java.net.URL
import org.scalatest.matchers.should.Matchers
import com.project.webcrawler.{URLHeuristics, URLHeuristicsImpl}

class URLHeuristicsImplTest extends AnyFlatSpec with Matchers {

  "getPriority" should "return correct priority depending on URL properties" in {
    val urlHeuristics = new URLHeuristicsImpl()
    val httpsUrl = new URL("https://example.com/path/to/hi/imlong//resource")
    val httpUrl = new URL("http://example.com/path/resource")

    urlHeuristics.getPriority(httpsUrl) should be < urlHeuristics.getPriority(httpUrl)
  }

  "shouldBeIgnored" should "ignore URLs based on certain conditions" in {
    val urlHeuristics = new URLHeuristicsImpl()

    // https and http protocol should not be ignored
    urlHeuristics.shouldBeIgnored(new URL("https://example.com")) shouldBe false
    urlHeuristics.shouldBeIgnored(new URL("http://example.com")) shouldBe false

    // blacklisted domain should be ignored
    urlHeuristics.shouldBeIgnored(new URL("http://malicious.com")) shouldBe true

    // blacklisted extension should be ignored
    urlHeuristics.shouldBeIgnored(new URL("http://example.com/image.jpg")) shouldBe true
  }
}