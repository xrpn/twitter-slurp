package com.xrpn.jhi

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.ActorSystem
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import better.files.{File => ScalaFile}
import com.typesafe.scalalogging.LazyLogging
import com.xrpn.jhi.aka.StatsActors
import com.xrpn.jhi.aks.StatsStreamer
import com.xrpn.jhi.http.HttpStatsServiceRestApi
import com.xrpn.jhi.t4j.T4JStreamer._
import com.xrpn.jhi.util.ConfigurableObject

import scala.concurrent.Future

class MockStreamer(materializer: ActorMaterializer) extends LazyLogging {

  import com.xrpn.jhi.json.SpritzerSampleFilenamesAndMore._

  private val linesInSamples = new AtomicInteger(0)

  def fillBuffer() = {

    val srcUrlSample01 = this.getClass.getClassLoader.getResource(fileNameSample01)
    val srcUrlSample02 = this.getClass.getClassLoader.getResource(fileNameSample02)
    val allSamples = List(srcUrlSample01.toURI, srcUrlSample02.toURI)

    rawBuffer.clear()
    linesInSamples.set(0)
    allSamples.foreach(sample => {
      val jsonFile = ScalaFile(sample)
      if (verbose) println(sample)
      jsonFile.lines.foreach(line => {
        if (rawBuffer.add(line)) linesInSamples.incrementAndGet()
      })
      if (verbose) println(s"added ${linesInSamples.intValue()}")
    })

  }

  import StatsStreamer._

  implicit val matl = materializer

  val countNonEmpty = new AtomicInteger(0)
  lazy val composedFlow = osa2trdCounted via trdCountedWithPayload via trdStatsDispatcher

  def runStreamer() = {
    fillBuffer()
    composedFlow.runWith(Sink.fold(0)((acc, trdac) => {
      val runningCount = acc + trdac.count
      if (0 == runningCount % 100) logger.info(s"at $runningCount")
      runningCount
    }))
  }

}

object MainTest extends App with RequestTimeout with ConfigurableObject {

  val host = getAkkaConfig.getString("http.host")
  // Gets the host and a port from the configuration
  val port = getAkkaConfig.getInt("http.port")

  val httpSystemName = "tweets-slurper-http-test"

  implicit val system = ActorSystem(httpSystemName)
  implicit val ec = system.dispatcher
  implicit val materializer = ActorMaterializer()

  StatsActors.initAllActors
  Thread.sleep(1000)
  new MockStreamer(StatsActors.statsMaterializer).runStreamer()

  val api = new HttpStatsServiceRestApi(system, requestTimeout(getAkkaConfig)).scRoutes // the RestApi provides a Route

  val bindingFuture: Future[ServerBinding] =
    Http().bindAndHandle(api, host, port) //Starts the HTTP server

  val log = Logging(system.eventStream, httpSystemName)
  bindingFuture.map { serverBinding =>
    log.info(s"RestApi bound to ${serverBinding.localAddress} ")
  }.onFailure {
    case ex: Exception =>
      log.error(ex, "Failed to bind to {}:{}!", host, port)
      system.terminate()
  }
}


