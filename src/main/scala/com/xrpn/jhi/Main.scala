package com.xrpn.jhi

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.ActorSystem
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import akka.util.Timeout
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import com.xrpn.jhi.aka.StatsActors
import com.xrpn.jhi.aks.StatsStreamer
import com.xrpn.jhi.http.HttpStatsServiceRestApi
import com.xrpn.jhi.t4j.T4JInstrumentedListener
import com.xrpn.jhi.t4j.T4JStreamer._
import com.xrpn.jhi.util.ConfigurableObject

import scala.concurrent.Future

/**
  * Build the streamer and connect it to the spritzer incoming feed.
  */
class DataStreamer(materializer: ActorMaterializer) extends LazyLogging {

  import StatsStreamer._

  implicit val matl = materializer

  /**
    * The streaming graph, sans the Sink.
    */
  lazy val composedFlow = osa2trdCounted via trdCountedWithPayload via trdStatsDispatcher

  logger.info("starting spritzer messages consumer")

  /**
    * The sink will count the items that make it to the end of the stream, and
    * print to log.
    */
  def runStreamer() = {
    composedFlow.runWith(Sink.fold(0)((acc, trdac) => {
      val runningCount = acc + 1
      if (0 == runningCount % 1000) logger.info(s"at $runningCount")
      runningCount
    }))
  }

  Thread.sleep(1000)
  logger.info("starting spritzer messages producer")

    val statefulListener = new T4JInstrumentedListener
//  instrumentedBufferingRawSampler(statefulListener, 80000) // listen statefully for a time, and then shut down
    instrumentedBufferingRawSampler(statefulListener) // listen statefully until stopped (Ctrl-C)
//  bufferingRawSampler(80000) // listen statelessly for a time, and then shut down
//  bufferingRawSampler() // listen statelessly until stopped (Ctrl-C)
}

object Main extends App with RequestTimeout with ConfigurableObject with LazyLogging {

  val host = getAkkaConfig.getString("http.host") // Gets the host and a port from the configuration
  val port = getAkkaConfig.getInt("http.port")

  val httpSystemName = "tweets-slurper-http"

  implicit val system = ActorSystem(httpSystemName)
  implicit val ec = system.dispatcher
  implicit val materializer = ActorMaterializer()

  logger.info("starting stats system")
  StatsActors.initAllActors
  Thread.sleep(1000)

  logger.info("starting web server")
  val api = new HttpStatsServiceRestApi(system, requestTimeout(getAkkaConfig)).scRoutes
  val bindingFuture: Future[ServerBinding] =
    Http().bindAndHandle(api, host, port) //Starts the HTTP server
  Thread.sleep(1000)

  val httpSystemLog =  Logging(system.eventStream, httpSystemName)
  bindingFuture.map { serverBinding =>
    httpSystemLog.info(s"RestApi bound to ${serverBinding.localAddress} ")
  }.onFailure {
    case ex: Exception =>
      httpSystemLog.error(ex, "Failed to bind to {}:{}!", host, port)
      system.terminate()
  }

  new DataStreamer(StatsActors.statsMaterializer).runStreamer()
}

trait RequestTimeout {
  import scala.concurrent.duration._
  def requestTimeout(config: Config): Timeout = { //<co id="ch02_timeout_spray_can"/>
    val t = config.getString("akka.http.server.request-timeout")
    val d = Duration(t)
    FiniteDuration(d.length, d.unit)
  }
}

