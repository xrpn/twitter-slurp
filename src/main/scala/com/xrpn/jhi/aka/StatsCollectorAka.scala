package com.xrpn.jhi.aka

import akka.actor._
import akka.event.Logging
import akka.pattern.{ask, pipe}
import com.xrpn.jhi.aka.ReaperAka.WatchMe
import com.xrpn.jhi.aka.StatsActors._

import scala.concurrent.Future

object StatsCollectorAka {

  sealed trait StreamingSTS

  case object Ping extends StreamingSTS
  case object CollectPercent extends StreamingSTS
  case object CollectMeanTime extends StreamingSTS
  case object CollectPops4Emoji extends StreamingSTS
  case object CollectPops4HashTag extends StreamingSTS
  case object CollectPops4Tld extends StreamingSTS
  case object CollectTweetCount extends StreamingSTS
  case class JsonServiceTime(result: String) extends StreamingSTS
  case class JsonServiceCount(result: String) extends StreamingSTS
  case class TweetArrivalInterval(result: String) extends StreamingSTS
  case class TweetArrivalCount(result: String) extends StreamingSTS
  case class Percent(result: String) extends StreamingSTS
  case class PercentCount(result: String) extends StreamingSTS
  case class Popularity(result: String) extends StreamingSTS
  case class PopularityCardinality(result: String) extends StreamingSTS

  val props1 = Props[StatsCollectorAka]
  val defaultActorName = "stats-collector"
  val brk = '\n'
  val brkStr = brk.toString

}

/**
  * Hub for gathering information to forward to downstream clients.  Relieves
  * the downstream client from having to understand how the Stats components
  * are internally broken up.
  */
class StatsCollectorAka() extends Actor {

  import StatsCollectorAka._
  implicit val ec = context.dispatcher

  val tag = defaultActorName
  val log =  Logging(context.system.eventStream, tag)

  StatsActors.getStatsReaper ! WatchMe(self)

  // actors that tally stats to be collected

  private lazy val mst4rawjsonAka: ActorSelection = getStatsActorByName(MessageServiceTimeAka.defaultActorName)
  private lazy val tai4tweetAka: ActorSelection = getStatsActorByName(TweetArrivalIntervalAka.defaultActorName)
  private lazy val count4tweets: ActorSelection = getStatsActorByName(countOfTweetsAkaName)
  private lazy val pct4emoji: ActorSelection = getStatsActorByName(percentEmojiAkaName)
  private lazy val pct4url: ActorSelection = getStatsActorByName(percentUrlAkaName)
  private lazy val pct4purl: ActorSelection = getStatsActorByName(percentPhotourlAkaName)
  private lazy val pops4emoji: ActorSelection = getStatsActorByName(popularityEmojiAkaName)
  private lazy val pops4hashtag: ActorSelection = getStatsActorByName(popularityHastagAkaName)
  private lazy val pops4tldomain: ActorSelection = getStatsActorByName(popularityTldomainAkaName)

  def receive = {
    case Ping =>
      log.info("alive and pinging all sources")
      StatsActors.pingAllStatsActors()

    case CollectPercent =>
      val polls: Future[(String, String, String)] = for {
        emoPct <- (pct4emoji ? PercentAka.PercentInquiry).mapTo[Percent].map(pct => pct.result)
        urlPct <- (pct4url ? PercentAka.PercentInquiry).mapTo[Percent].map(pct => pct.result)
        purlPct <- (pct4purl ? PercentAka.PercentInquiry).mapTo[Percent].map(pct => pct.result)
      } yield (emoPct, urlPct, purlPct)
      polls.mapTo[Tuple3[String,String,String]].map( tt => s"${tt._1}$brkStr${tt._2}$brkStr${tt._3}") pipeTo sender

    case CollectMeanTime =>
      val polls: Future[(String, String)] = for {
        mst <- (mst4rawjsonAka ? MessageServiceTimeAka.ServiceTimeInquiry).mapTo[JsonServiceTime].map(mst => mst.result)
        tai <- (tai4tweetAka ? TweetArrivalIntervalAka.AllPeriodArrivalIntervalInquiry).mapTo[TweetArrivalInterval].map(tai => tai.result)
      } yield (mst, tai)
      polls.mapTo[Tuple2[String,String]].map( tt => s"${tt._1}$brkStr${tt._2}") pipeTo sender

    case CollectPops4Emoji =>
      (pops4emoji ? PopularityAka.PopularityInquiry).mapTo[Popularity].map(pops => pops.result) pipeTo sender

    case CollectPops4HashTag =>
      (pops4hashtag ? PopularityAka.PopularityInquiry).mapTo[Popularity].map(pops => pops.result) pipeTo sender

    case CollectPops4Tld =>
      (pops4tldomain ? PopularityAka.PopularityInquiry).mapTo[Popularity].map(pops => pops.result) pipeTo sender

    case CollectTweetCount =>
      (count4tweets ? PercentAka.CardinalityInquiry).mapTo[PercentCount].map(cnt => cnt.result) pipeTo sender

  }
}

