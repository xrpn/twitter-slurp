package com.xrpn.jhi.aks

import java.net.{URI, URL}
import java.util.concurrent.atomic.AtomicInteger

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import better.files.{File => ScalaFile}
import com.xrpn.jhi.aka.StatsActors
import com.xrpn.jhi.t4j.T4JStreamer._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}

import scala.concurrent.Await
import scala.concurrent.duration._

/**
  * Created by alsq on 11/22/16.
  */
class StatsStreamerTest  extends FunSuite with BeforeAndAfterEach with BeforeAndAfterAll {

  import com.xrpn.jhi.json.SpritzerSampleFilenamesAndMore._

  private var srcUrlSample01: URL = _
  private var srcUrlSample02: URL = _
  private var allSamples: List[URI] = Nil
  private val linesInSamples = new AtomicInteger(0)
  private val tweetsInSamples = new AtomicInteger(-1)
  private val tol = 10
  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()

  override def beforeAll() {
    srcUrlSample01 = this.getClass.getClassLoader.getResource(fileNameSample01)
    srcUrlSample02 = this.getClass.getClassLoader.getResource(fileNameSample02)
    allSamples = List(srcUrlSample01.toURI,srcUrlSample02.toURI)
  }

  override def beforeEach(): Unit = {
    rawBuffer.clear()
    linesInSamples.set(0)
    allSamples.foreach(sample => {
      val jsonFile = ScalaFile(sample)
      assert(jsonFile.exists)
      if (verbose) println(sample)
      jsonFile.lines.foreach(line => {
        if (rawBuffer.add(line)) linesInSamples.incrementAndGet()
      })
      if (verbose) println(s"added ${linesInSamples.intValue()}")
    })
  }

  test ("smoke") {
    assert (1 === 1)
    assert (1 !== Option(1))
    assert(!rawBuffer.isEmpty)
  }

  // TODO find a better manner to terminate an infinite stream.

  /*
   NOTE:  ALL tests must run synchronously and block waiting for completion since they share the rawBuffer.
   This is an expedient strategy for testing not so much the correctness, but rather the viability of the
   streaming graph.  It is afflicted by race conditions because of the crudeness in terminating processing
   at, ahem, the end of a (theoretically, yes?) infinite stream.  I'm sure there must be a better solution,
   but it's time to wrap up.  The MVP/POC is hopefully good enough; perfection is for tomorrow.
   */

  test ("simplistic (i.e. failure is non-critical), source") {
    import StatsStreamer._
    val countLine = new AtomicInteger(0)
    val countNone = new AtomicInteger(0)
    val composedFlow: Source[Option[String], NotUsed] = rawSource
    val start = System.currentTimeMillis()
    try {
      Await.result(composedFlow.runWith(Sink.foreach {
        case oraw@Some(ll) =>
          if (isViewOn) println(oraw)
          countLine.incrementAndGet()
        case None =>
          if (rawBuffer.isEmpty) throw new IllegalArgumentException()
          countNone.incrementAndGet()
      }), 1 minute)
      throw new IllegalStateException("did not throw")
    } catch {
      case iae:IllegalArgumentException =>
        val stop = System.currentTimeMillis()
        if(isPrintingAsserts) println(s"simplistic (i.e. failure is non-critical), source in ms ${stop - start}")
        if(isPrintingAsserts) println(s"lines: ${countLine.intValue()}")
        if(isPrintingAsserts) println(s"nones: ${countNone.intValue()}")
        assert(linesInSamples.intValue() == countLine.intValue())
      case e:Exception => fail(e)
    }
  }

  test ("simplistic (i.e. failure is non-critical), status aging") {
    import StatsStreamer._
    val countLine = new AtomicInteger(0)
    val countNone = new AtomicInteger(0)
    val composedFlow: Source[Option[StatusAging], NotUsed] = rawSource via ojson2osaWorker
    val start = System.currentTimeMillis()
    try {
      Await.result(composedFlow.runWith(Sink.foreach {
        case Some(ll) =>
          if (isViewOn) println(ll)
          countLine.incrementAndGet()
          if (rawBuffer.isEmpty) throw new IllegalArgumentException()
        case None =>
          countNone.incrementAndGet()
          if (rawBuffer.isEmpty) throw new IllegalArgumentException()
      }), 20 seconds)
      throw new IllegalStateException("did not throw")
    } catch {
      case iae:IllegalArgumentException =>
        val stop = System.currentTimeMillis()
        if(isPrintingAsserts) println(s"simplistic (i.e. failure is non-critical), status aging in ms ${stop - start}")
        if(isPrintingAsserts) println(s"status: ${countLine.intValue()}")
        if(isPrintingAsserts) println(s"nones: ${countNone.intValue()}")
        assert(132 == countLine.intValue())
        assert(54 == countNone.intValue())
        assert(countLine.intValue() + countNone.intValue() == linesInSamples.intValue())
        tweetsInSamples.set(countLine.intValue())
      case e:Exception => fail(e)
    }
  }

  test ("simplistic (i.e. failure is non-critical), trd") {
    import StatsStreamer._
    val countLine = new AtomicInteger(0)
    val composedFlow = rawSource via ojson2osaWorker via osa2trdFlow
    val start = System.currentTimeMillis()
    try {
      Await.result(composedFlow.runWith(Sink.foreach(trd => {
        if (isViewOn) println(trd)
        countLine.incrementAndGet()
        if (rawBuffer.isEmpty) throw new IllegalArgumentException()
      })), 1 minute)
      throw new IllegalStateException("did not throw")
    } catch {
      case iae: IllegalArgumentException =>
        val stop = System.currentTimeMillis()
        if(isPrintingAsserts) println(s"simplistic (i.e. failure is non-critical), trd in ms ${stop - start}")
        if(isPrintingAsserts) println(s"tweets: ${countLine.intValue()}")
        if (-1 < tweetsInSamples.intValue()) assert(tweetsInSamples.intValue() == countLine.intValue())
        assert((132 >= countLine.intValue()) && (countLine.intValue() >= 132-tol))
      case e: Exception => fail(e)
    }
  }

  test ("simplistic (i.e. failure is non-critical), counted") {
    import StatsStreamer._
    val countLine = new AtomicInteger(0)
    val composedFlow= osa2trdCounted
    val start = System.currentTimeMillis()
    try {
      Await.result(composedFlow.runWith(Sink.foreach(trd => {
        if (isViewOn) println(trd)
        countLine.incrementAndGet()
        if (rawBuffer.isEmpty) throw new IllegalArgumentException(""+trd._2)
      })), 1 minute)
      throw new IllegalStateException("did not throw")
    } catch {
      case iae: IllegalArgumentException =>
        val stop = System.currentTimeMillis()
        if(isPrintingAsserts) println(s"simplistic (i.e. failure is non-critical), trd counted in ms ${stop - start}")
        if(isPrintingAsserts) println(s"tweets: ${countLine.intValue()}")
        if(isPrintingAsserts) println(s"counted: ${iae.getMessage}")
        val counted = iae.getMessage.toInt
        if (-1 < tweetsInSamples.intValue()) {
          assert((tweetsInSamples.intValue() >= countLine.intValue()) && (countLine.intValue() >= (tweetsInSamples.intValue() - tol)))
          assert((tweetsInSamples.intValue() >= counted) && (counted >= (tweetsInSamples.intValue() - tol)))
        }
        else {
          assert((132 >= countLine.intValue()) && (countLine.intValue() >= 132-tol))
          assert((132 >= counted) && (counted >= 132-tol))
        }
      case e: Exception => fail(e)
    }
  }

  test ("simplistic (i.e. failure is non-critical), with payload, counted") {
    import StatsStreamer._
    val countNonEmpty = new AtomicInteger(0)
    val composedFlow= osa2trdCounted via trdCountedWithPayload
    val start = System.currentTimeMillis()
    try {
      Await.result(composedFlow.runWith(Sink.foreach(trdac => {
        if (isViewOn) println(trdac)
        assert(trdac.trda.trd.urls.nonEmpty ||
          trdac.trda.trd.purls.nonEmpty ||
          trdac.trda.trd.hts.nonEmpty ||
          trdac.trda.trd.emos.nonEmpty)
        countNonEmpty.incrementAndGet()
        if (rawBuffer.isEmpty) throw new IllegalArgumentException(""+trdac.count)
      })), 1 minute)
      throw new IllegalStateException("did not throw")
    } catch {
      case iae: IllegalArgumentException =>
        val stop = System.currentTimeMillis()
        if(isPrintingAsserts) println(s"simplistic (i.e. failure is non-critical), payload counted in ms ${stop - start}")
        if(isPrintingAsserts) println(s"non-empty tweets: ${countNonEmpty.intValue()}")
        if(isPrintingAsserts) println(s"counted: ${iae.getMessage}")
        val counted = iae.getMessage.toInt
        if (-1 < tweetsInSamples.intValue()) {
          assert((tweetsInSamples.intValue() >= counted) && (counted >= (tweetsInSamples.intValue() - tol)))
          assert((75 >= countNonEmpty.intValue()) && (countNonEmpty.intValue() >= 75 - tol))
        }
        else {
          assert((75 >= countNonEmpty.intValue()) && (countNonEmpty.intValue() >= 75 - tol))
          assert((132 >= counted) && (counted >= 132-tol))
        }
      case e: Exception => fail(e)
    }
  }

  test ("simplistic (i.e. failure is non-critical), with payload, dispatch") {
    import StatsStreamer._
    val countNonEmpty = new AtomicInteger(0)
    val composedFlow= osa2trdCounted via trdCountedWithPayload via trdStatsDispatcher
    val start = System.currentTimeMillis()
    try {
      Await.result(composedFlow.runWith(Sink.foreach(trdac => {
        if (isViewOn) println(trdac)
        assert(trdac.trda.trd.urls.nonEmpty ||
          trdac.trda.trd.purls.nonEmpty ||
          trdac.trda.trd.hts.nonEmpty ||
          trdac.trda.trd.emos.nonEmpty)
        countNonEmpty.incrementAndGet()
        if (rawBuffer.isEmpty) throw new IllegalArgumentException(""+trdac.count)
      })), 1 minute)
      throw new IllegalStateException("did not throw")
    } catch {
      case iae: IllegalArgumentException =>
        val stop = System.currentTimeMillis()
        if(isPrintingAsserts) println(s"simplistic (i.e. failure is non-critical), payload dispatched in ms ${stop - start}")
        if(isPrintingAsserts) println(s"non-empty tweets: ${countNonEmpty.intValue()}")
        if(isPrintingAsserts) println(s"counted: ${iae.getMessage}")
        val counted = iae.getMessage.toInt
        if (-1 < tweetsInSamples.intValue()) {
          assert((tweetsInSamples.intValue() >= counted) && (counted >= (tweetsInSamples.intValue() - tol)))
          assert((75 >= countNonEmpty.intValue()) && (countNonEmpty.intValue() >= 75 - tol))
        }
        else {
          assert((75 >= countNonEmpty.intValue()) && (countNonEmpty.intValue() >= 75 - tol))
          assert((132 >= counted) && (counted >= 132-tol))
        }
      case e: Exception => fail(e)
    }
  }

}
