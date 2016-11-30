package com.xrpn.jhi.http

import akka.actor.{Actor, ActorRef, ActorSelection, Props}
import akka.event.Logging
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import com.xrpn.jhi.aka.ReaperAka.WatchMe
import com.xrpn.jhi.aka.{StatsActors, StatsCollectorAka}

import scala.util.{Failure, Success, Try}

object HttpStatsServiceAka {

  /**
    * Enumeration-like entities matching the REST call granularity.  These are
    * the typed messages for the service actor.
    */
  sealed trait StatsServiceHTTP
  case object MeanTime extends StatsServiceHTTP
  case object TweetCount extends StatsServiceHTTP
  case object Percent extends StatsServiceHTTP
  case object TopEmoji extends StatsServiceHTTP
  case object TopHashtag extends StatsServiceHTTP
  case object TopTld extends StatsServiceHTTP
  case object Ping extends StatsServiceHTTP

  def props1(to: Timeout) = Props(classOf[HttpStatsServiceAka],to)
  val defaultActorName = "http-stats-service-actor"

  /**
    * Hello message.
    */
  val pingReply = s"""
                     |Stats Collector is online for:
                     |GET meantime
                     |GET percent
                     |GET tweetcount
                     |GET topemoji
                     |GET tophashtag
                     |GET toptld
                     |""".stripMargin

}

/**
  * The actor that powers the REST service visible to http clients.
  * Created by alsq on 11/29/16.
  */
class HttpStatsServiceAka(val timeout: Timeout)  extends Actor {

  import HttpStatsServiceAka._

  implicit val to = timeout
  implicit val ec = context.dispatcher

  val tag = defaultActorName
  val log =  Logging(context.system.eventStream, tag)

  StatsActors.getStatsReaper ! WatchMe(self)

  private lazy val statsCollector: ActorSelection = StatsActors.getStatsActorByName(StatsCollectorAka.defaultActorName)

  def receive = {
    case Ping => sender ! pingReply
    case Percent => (statsCollector ? StatsCollectorAka.CollectPercent).mapTo[String] pipeTo sender
    case TweetCount => (statsCollector ? StatsCollectorAka.CollectTweetCount).mapTo[String] pipeTo sender
    case MeanTime => (statsCollector ? StatsCollectorAka.CollectMeanTime).mapTo[String] pipeTo sender
    case TopEmoji => (statsCollector ? StatsCollectorAka.CollectPops4Emoji).mapTo[String] pipeTo sender
    case TopHashtag => (statsCollector ? StatsCollectorAka.CollectPops4HashTag).mapTo[String] pipeTo sender
    case TopTld => (statsCollector ? StatsCollectorAka.CollectPops4Tld).mapTo[String] pipeTo sender
    case mm@_ => sender ! s"unknown message ${mm.toString}"
  }

}
