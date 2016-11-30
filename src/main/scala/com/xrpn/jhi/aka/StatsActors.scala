package com.xrpn.jhi.aka

import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, ActorSelection, ActorSystem}
import akka.event.Logging
import akka.stream.ActorMaterializer
import akka.util.Timeout
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.duration.FiniteDuration

/**
  * System level functionality for the Stats component.  This is the architectural
  * part that keeps state of current counts and other relevant information.
  * Created by alsq on 11/22/16.
  */
object StatsActors extends LazyLogging {

  val statsSystemName = "tweets-slurper-system"
  implicit val statsSystem = ActorSystem(statsSystemName)
  implicit val statsMaterializer = ActorMaterializer()
  implicit val statsDefaultTimeout = Timeout(FiniteDuration(10, TimeUnit.SECONDS))

  val statsSystemLog = Logging(statsSystem.eventStream, statsSystemName)

  /**
    * Actor names, individually.
    */
  val countOfTweetsAkaName = "tweet-counter"
  val percentEmojiAkaName = "emoji-pct"
  val percentPhotourlAkaName = "photourl-pct"
  val percentUrlAkaName = "url-pct"
  val popularityEmojiAkaName = "emoji-popularity"
  val popularityHastagAkaName = "hashtag-popularity"
  val popularityTldomainAkaName = "tldomain-popularity"

  /**
    * Actor names, as a Set.
    */
  val statsAkaNames = Set(
    StatsCollectorAka.defaultActorName,
    ReaperAka.defaultActorName,
    MessageServiceTimeAka.defaultActorName,
    TweetArrivalIntervalAka.defaultActorName,
    countOfTweetsAkaName,
    percentEmojiAkaName,
    percentPhotourlAkaName,
    percentUrlAkaName,
    popularityEmojiAkaName,
    popularityHastagAkaName,
    popularityTldomainAkaName
  )

  /**
    * Enumeration-like structure for sources of statistical data
    * that need be kept track of.
    */
  sealed trait StatSource extends Serializable
  case object MessageServiceTime extends StatSource
  case object TweetArrivalInterval extends StatSource
  case object PercentEmoji extends StatSource
  case object TweetCounter extends StatSource
  case object PercentUrl extends StatSource
  case object PercentPhotourl extends StatSource
  case object PopularityEmoji extends StatSource
  case object PopularityHashtag extends StatSource
  case object PopularityTldomain extends StatSource

  /**
    * Associate stats sources with the actor names that accumulate their state.
    */
  val statsSrcAkaNames = Map[StatSource, String](
    MessageServiceTime -> MessageServiceTimeAka.defaultActorName,
    TweetArrivalInterval -> TweetArrivalIntervalAka.defaultActorName,
    TweetCounter -> countOfTweetsAkaName,
    PercentEmoji -> percentEmojiAkaName,
    PercentPhotourl -> percentPhotourlAkaName,
    PercentUrl -> percentUrlAkaName,
    PopularityEmoji -> popularityEmojiAkaName,
    PopularityHashtag -> popularityHastagAkaName,
    PopularityTldomain -> popularityTldomainAkaName
  )

  /**
    * Actors may be initialized at most once.
    */
  private lazy val inited: Unit = {
    statsSystemLog.info("initializing stats actors")
    val systemReaper: ActorRef = statsSystem.actorOf(ReaperAka.props1, ReaperAka.defaultActorName)
    Thread.sleep(200) // crude; allow the reaper to start, as it is referenced in the following
    val statsCollectorAka: ActorRef = statsSystem.actorOf(StatsCollectorAka.props1, StatsCollectorAka.defaultActorName)
    val messageServiceTime: ActorRef = statsSystem.actorOf(MessageServiceTimeAka.props1, MessageServiceTimeAka.defaultActorName)
    val tweetArrivalInterval: ActorRef = statsSystem.actorOf(TweetArrivalIntervalAka.props1, TweetArrivalIntervalAka.defaultActorName)
    val tweetCounter: ActorRef = statsSystem.actorOf(PercentAka.props1(TweetCounter), countOfTweetsAkaName)
    val emojiPercent: ActorRef = statsSystem.actorOf(PercentAka.props1(PercentEmoji), percentEmojiAkaName)
    val urlPercent: ActorRef = statsSystem.actorOf(PercentAka.props1(PercentUrl), percentUrlAkaName)
    val photourlPercent: ActorRef = statsSystem.actorOf(PercentAka.props1(PercentPhotourl), percentPhotourlAkaName)
    val emojiPopularity: ActorRef = statsSystem.actorOf(PopularityAka.props1(PopularityEmoji), popularityEmojiAkaName)
    val hashtagPopularity: ActorRef = statsSystem.actorOf(PopularityAka.props1(PopularityHashtag), popularityHastagAkaName)
    val tldomainPopularity: ActorRef = statsSystem.actorOf(PopularityAka.props1(PopularityTldomain), popularityTldomainAkaName)
    statsSystemLog.info("initialized stats actors")
  }

  /**
    * Invoke on StatActors to make sure all stats actors are initialized.  Since
    * unreferenced objects just don't happen, this is also a benign hack to make
    * sure that the stats actors will be running when needed.
    */
  def initAllActors() = inited

  /**
    * Retrieve something you can send messages to.
    *
    * @param akaName the name of the stats actor
    * @return a messaqgeable entity
    */
  def getStatsActorByName(akaName: String): ActorSelection = {
    assert(statsAkaNames.contains(akaName))
    val fqAkaName = s"user/$akaName"
    statsSystem.actorSelection(fqAkaName)
  }

  /**
    * Retrieve the reaper.  Useful for testing and for orderly shutdown.
    */
  def getStatsReaper: ActorSelection = statsSystem.actorSelection(s"user/${ReaperAka.defaultActorName}")

  /**
    * (for testing)
    *
    * @return
    */
  private[aka] def getAllStatsActors: List[ActorSelection] = List(
    // DON'T put the Reaper here; this is used to tell the PoisonPill on termination
    getStatsActorByName(StatsCollectorAka.defaultActorName),
    getStatsActorByName(MessageServiceTimeAka.defaultActorName),
    getStatsActorByName(TweetArrivalIntervalAka.defaultActorName),
    getStatsActorByName(countOfTweetsAkaName),
    getStatsActorByName(percentEmojiAkaName),
    getStatsActorByName(percentUrlAkaName),
    getStatsActorByName(percentPhotourlAkaName),
    getStatsActorByName(popularityEmojiAkaName),
    getStatsActorByName(popularityHastagAkaName),
    getStatsActorByName(popularityTldomainAkaName)
  )

  def pingAllStatsActors() = {
    // don't put StatsCollectorAka in here, lest it pings itself in an infinite loop
    val mst4rawjsonAka = getStatsActorByName(MessageServiceTimeAka.defaultActorName)
    mst4rawjsonAka ! MessageServiceTimeAka.Ping
    val tai4tweetAka = getStatsActorByName(TweetArrivalIntervalAka.defaultActorName)
    tai4tweetAka ! TweetArrivalIntervalAka.Ping
    val count4tweets = getStatsActorByName(countOfTweetsAkaName)
    count4tweets ! PercentAka.Ping
    val pct4emoji = getStatsActorByName(percentEmojiAkaName)
    pct4emoji ! PercentAka.Ping
    val pct4url = getStatsActorByName(percentUrlAkaName)
    pct4url ! PercentAka.Ping
    val pct4purl = getStatsActorByName(percentPhotourlAkaName)
    pct4purl ! PercentAka.Ping
    val pops4emoji = getStatsActorByName(popularityEmojiAkaName)
    pops4emoji ! PopularityAka.Ping
    val pops4hashtag = getStatsActorByName(popularityHastagAkaName)
    pops4hashtag ! PopularityAka.Ping
    val pops4tldomain = getStatsActorByName(popularityTldomainAkaName)
    pops4tldomain ! PopularityAka.Ping
  }
}
