package com.xrpn.jhi.json

import java.net.{URI, URL}

import better.files.{File => ScalaFile}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}

object SpritzerSampleFilenamesAndMore {

  val fileNameSample02 = "spritzersample02.txt"
  val fileNameSample01 = "spritzersample01.txt"

  val verbose: Boolean = false
  val isViewOn: Boolean = false
  val isPrintingAsserts: Boolean = false

}

/**
  * Created by alsq on 11/19/16.
  */
class JsonWranglerTest extends FunSuite with BeforeAndAfterEach with BeforeAndAfterAll {

  import JsonWrangler._
  import SpritzerSampleFilenamesAndMore._

  private var srcUrlSample01: URL = _
  private var srcUrlSample02: URL = _
  private var allSamples: List[URI] = Nil

  override def beforeAll() {
    srcUrlSample01 = this.getClass.getClassLoader.getResource(fileNameSample01)
    srcUrlSample02 = this.getClass.getClassLoader.getResource(fileNameSample02)
    allSamples = List(srcUrlSample01.toURI,srcUrlSample02.toURI)
  }

  test ("smoke") {
    assert (1 === 1)
    assert (1 !== Option(1))
  }


  test(s"exists hashTag") {
    implicit val debug = false
    allSamples.foreach(sample => {
      val jsonFile = ScalaFile(sample)
      assert(jsonFile.exists)
      if (verbose) println(sample)
      val aut = jsonFile.lines.map(line => {
        val hts: List[String] = JsonWrangler.raw2hashtags(line)
        if (hts.nonEmpty && isViewOn) println(JsonWrangler.raw2hashtags(line))
        hts.nonEmpty
      })
      assert(aut.nonEmpty && aut.exists(p => p))
    })
  }

  test(s"exists url") {
    implicit val debug = false
    allSamples.foreach(sample => {
      val jsonFile = ScalaFile(sample)
      assert(jsonFile.exists)
      if (verbose) println(sample)
      val aut = jsonFile.lines.map(line => {
        val url: List[String] = JsonWrangler.raw2url(line)
        if (url.nonEmpty && isViewOn) println(url)
        url.nonEmpty
      })
      assert(aut.nonEmpty && aut.exists(p => p))
    })
  }

  test(s"hashtag agreement") {
    implicit val debug = false
    allSamples.foreach(sample => {
      val jsonFile = ScalaFile(sample)
      assert(jsonFile.exists)
      if (verbose) println(sample)
      jsonFile.lines.foreach(line => {
        val o2ht = raw2statusObj(line).flatMap(statusObj2hashtags).toSet
        val s2ht = raw2statusStr(line).flatMap(statusStr2hashtags).toSet
        if (!o2ht.equals(s2ht)) {
          println(s"hashtag agreement error")
          println(s"msg: $line")
          println(s"status obj: ${raw2statusObj(line).toString}")
          println(s"status str: ${raw2statusStr(line)}")
        }
        assert(o2ht === s2ht)
      })
    })
  }

  test("url agreement") {
    implicit val debug = false
    allSamples.foreach(sample => {
      val jsonFile = ScalaFile(sample)
      assert(jsonFile.exists)
      if (verbose) println(sample)
      jsonFile.lines.foreach(line => {
        val o2url = raw2statusObj(line).flatMap(s => statusObj2url(s,false)).toSet
        val s2url = raw2statusStr(line).flatMap(statusStr2url).toSet
        // see note in statusObj2url with regard to parsing truncated urls
        val isSame = s2url.forall(s2u => o2url.exists(o2u => o2u.toLowerCase.startsWith(s2u.toLowerCase())))
        if (!isSame) {
          println(s"url agreement error")
          println(s"msg: $line")
          println(s"status obj: ${raw2statusObj(line).toString}")
          println(s"status str: ${raw2statusStr(line)}")
        }
        assert(isSame)
      })
    })
  }

  test("exists photourl") {
    implicit val debug = false
    allSamples.foreach(sample => {
      val jsonFile = ScalaFile(sample)
      assert(jsonFile.exists)
      if (verbose) println(sample)
      val aut = jsonFile.lines.map(line => {
        val url: List[String] = JsonWrangler.raw2photoUrl(line)
        if (url.nonEmpty && isViewOn) println(url)
        url.nonEmpty
      })
      assert(aut.nonEmpty && aut.exists(p => p))
    })
  }

  test("exists emoji") {
    implicit val debug = false
    allSamples.foreach(sample => {
      val jsonFile = ScalaFile(sample)
      assert(jsonFile.exists)
      if (verbose) println(sample)
      val aut = jsonFile.lines.map(line => {
        val emoji: List[String] = JsonWrangler.raw2emoji(line)
        if (emoji.nonEmpty && isViewOn) println(emoji)
        emoji.nonEmpty
      })
      assert(aut.nonEmpty && aut.exists(p => p))
    })
  }
}
