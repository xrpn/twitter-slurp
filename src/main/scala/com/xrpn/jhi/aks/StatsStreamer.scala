package com.xrpn.jhi.aks

import akka.NotUsed
import akka.actor.ActorSelection
import akka.stream.scaladsl.{Flow, Source}
import com.typesafe.scalalogging.LazyLogging
import com.xrpn.jhi.aka.{MessageServiceTimeAka, PercentAka, PopularityAka, StatsActors, TweetArrivalIntervalAka}
import com.xrpn.jhi.json.JsonWrangler._
import com.xrpn.jhi.t4j.T4JStreamer.rawBuffer
import com.xrpn.jhi.util.{ConfigurableObject, StreamingAks}
import twitter4j.Status

/**
  * Created by alsq on 11/18/16.
  */

/**
  * This is in the interest of expediency.
  * @param jq
  */
case class IterableJQ(val jq: java.util.Queue[String], msNoneArrivalTime: Int) extends scala.collection.immutable.Iterable[Option[String]] {
  lazy val iteratorImpl: Iterator[Option[String]] = new Iterator[Option[String]] {
    override def hasNext: Boolean = true
    override def next(): Option[String] = Option(jq.poll())
    match {
        case None =>
          // crude flow control: prevent busy-wait while jq is empty
          Thread.sleep(msNoneArrivalTime)
          None
        case ret@Some(_) =>
          ret
      }
  }
  override def iterator: Iterator[Option[String]] = iteratorImpl
  // not a sure thing, as extractions may happen at any time.
  def mayHaveData: Boolean = !jq.isEmpty

}

/**
  * A tweet with timestamp.
  * @param status a Tweet according to twitter4j
  * @param aging timestamp at creation
  */
case class StatusAging(status: Status, aging: Long)

/**
  * Summary for our purposes of the relevant information in a Tweet.
  * @param urls list of urls, possibly empty
  * @param purls list of picture urls, possibly empty
  * @param hts list of hashtags, possibly empty
  * @param emos list of emojis, possibly empty
  */
case class TweetRawData(urls: List[String], purls: List[String], hts: List[String], emos: List[String]) {}

/**
  * TweetRawData with a timestamp.
  * @param trd TweetRawData
  * @param aging timestamp at creation
  */
case class TweetRawDataAging(trd: TweetRawData, aging: Long) {}

/**
  * TweetRawDataAging with a sequence number representing its order of processing,
  * which is not necessarily the same as the order of arrival, but the difference
  * here is not relevant.  This number is as well the cumulative number of tweets
  * we know have already arrived at that time.
  * @param trda TweetRawDataAging
  * @param count sequence number at creation
  */
case class TweetRawDataAgingCounted(trda: TweetRawDataAging, count: Int) {}

/**
  * Build the pieces that will stream the incoming content from the spritzer.
  */
object StatsStreamer extends ConfigurableObject with LazyLogging {

  import MessageServiceTimeAka._
  import PercentAka._
  import PopularityAka._
  import StreamingAks._
  import TweetArrivalIntervalAka._

  /**
    * Configuration key prefix.
    */
  val myKey = "stats-streamer"

  /**
    * How to retrieve jsonWorkers from the configuration file.
    */
  val jsonWorkersKey = "json-parse-workers"

  /**
    * The number of concurrent workers for parsing raw json messages
    */
  val jsonWorkers = akkaConfig.getInt(s"$myKey.$jsonWorkersKey")

  /**
    * How to retrieve noneArrivalInterval from the configuration file.
    */
  val noneArrivalIntervalKey = "none-arrivals-lapse-ms"

  /**
    * The number of milliseconds to wait before returning a value from the raw
    * buffer, if the raw buffer is empty at the time the request is made.  This
    * is a simple implementation of flow control and prevents busy-waiting.
    */
  val noneArrivalInterval = akkaConfig.getInt(s"$myKey.$noneArrivalIntervalKey")

  /**
    * Utility raw data entity.
    */
  val emptyTrd = TweetRawData(Nil,Nil,Nil,Nil)

  // actors that tally stats from incoming data

  private lazy val mst4rawjsonAka: ActorSelection = StatsActors.getStatsActorByName(MessageServiceTimeAka.defaultActorName)
  private lazy val tai4tweetAka: ActorSelection = StatsActors.getStatsActorByName(TweetArrivalIntervalAka.defaultActorName)
  private lazy val count4tweets: ActorSelection = StatsActors.getStatsActorByName(StatsActors.countOfTweetsAkaName)
  private lazy val pct4emoji: ActorSelection = StatsActors.getStatsActorByName(StatsActors.percentEmojiAkaName)
  private lazy val pct4url: ActorSelection = StatsActors.getStatsActorByName(StatsActors.percentUrlAkaName)
  private lazy val pct4purl: ActorSelection = StatsActors.getStatsActorByName(StatsActors.percentPhotourlAkaName)
  private lazy val pops4emoji: ActorSelection = StatsActors.getStatsActorByName(StatsActors.popularityEmojiAkaName)
  private lazy val pops4hashtag: ActorSelection = StatsActors.getStatsActorByName(StatsActors.popularityHastagAkaName)
  private lazy val pops4tldomain: ActorSelection = StatsActors.getStatsActorByName(StatsActors.popularityTldomainAkaName)

  // don't ask :)
  private implicit val debug = false

  /**
    * List to Option translator for use with a stream of Option[String]
    * @return Twitter4j object representation of a tweet, "Status", as a list
    *         with one element if the raw json is a tweet, or an empty list
    *         otherwise.
    */
  private def oraw2oStatusAging: PartialFunction[Option[String], Option[StatusAging]] = {
    case Some(json) => raw2statusObj(json) match {
      case Nil => None
      case ss :: Nil => {
        val aging = System.currentTimeMillis()
        // tally the mean service time for extracting lines from the raw buffer
        mst4rawjsonAka ! RawJsonServiced(aging)
        Option(StatusAging(ss,aging))
      }
      case _ => throw new RuntimeException()
    }
  }

  /**
    * Extractor for use with a stream of Option[StatusAging], i.e. the timestamped
    * Twitter4j representation of a tweet.
    * @return a simpler representation of only the content relevant to this POC.  This
    *         representation may in fact be empty if the original tweet has no content
    *         of interest here.
    */
  private def oStatusObj2TweetRawDataAged: PartialFunction[Option[StatusAging],TweetRawDataAging] = {
    case Some(sa) =>
      val urls = statusObj2url(sa.status)
      val purls = statusObj2photoUrl(sa.status)
      val hts = statusObj2hashtags(sa.status)
      val emos = statusObj2emoji(sa.status)
      // tally the mean arrival interval of actual tweets (as opposed to other raw message types)
      tai4tweetAka ! TweetArrival(sa.aging)
      TweetRawDataAging(TweetRawData(urls,purls,hts,emos),sa.aging)
  }

  /**
    * The source of raw json messages
    */
  val rawSource: Source[Option[String], NotUsed] = Source(IterableJQ(rawBuffer,noneArrivalInterval))

  /**
    * Worker for initial processing stage of raw json messages.  If the message is a tweet then turn it
    * into a timestamped object; else none.
    */
  val ojson2osaWorker: Flow[Option[String],Option[StatusAging],NotUsed] = Flow[Option[String]].collect(oraw2oStatusAging)

  /**
    * Balanced concurrent workers for initial processing stage of raw json messages.
    */
  val ojson2osaWithWorkers: Flow[Option[String],Option[StatusAging],NotUsed] = balancer(ojson2osaWorker,jsonWorkers)

  /**
    * (used for testing)
    */
  private[aks] val osa2trdFlow: Flow[Option[StatusAging], TweetRawDataAging, NotUsed] = Flow[Option[StatusAging]].collect(oStatusObj2TweetRawDataAged)

  /**
    * For each timestamped tweet object, extract the (possibly empty) tweet raw data of interest
    * here.  The timestamped raw data comprises (possibly empty) collections of urls, picture urls,
    * hashtags, emojis
    */
  val osa2trdSrc: Source[TweetRawDataAging, NotUsed] = (rawSource via ojson2osaWithWorkers).collect(oStatusObj2TweetRawDataAged)

  /**
    * Zip each tweet raw data with its sequence number.  The sequence number is the
    * current count of tweets received (offset 1), and also represents the arrival
    * order of the tweet from which the raw data was obtained.
    */
  val osa2trdCounted: Source[(TweetRawDataAging, Int), NotUsed] = counted(osa2trdSrc)

  /**
    * Remove from further processing the tweet raw data for which all collections are
    * empty (i.e. remove all tweets with no urls, no purls, no hastags, no emojis).  Retain
    * the associated sequence number for the tweet raw data that has some content, for
    * further processing.
    */
  val trdCountedWithPayload: Flow[(TweetRawDataAging, Int),TweetRawDataAgingCounted,NotUsed] = Flow[(TweetRawDataAging, Int)]
    .filterNot{ case (trda, _) => trda.trd.equals(emptyTrd) }
    .map{ case (trda, count) => TweetRawDataAgingCounted(trda, count) }

  /**
    * Dispatch items of interest to tallying actors.
    */
  val trdStatsDispatcher: Flow[TweetRawDataAgingCounted,TweetRawDataAgingCounted,NotUsed] = Flow[TweetRawDataAgingCounted]
    .map( trdac => {
      count4tweets ! PercentFound(trdac.count)
      if (trdac.trda.trd.emos.nonEmpty) {
        // logger.info(s"emos.nonEmpty at ${trdac.toString}")
        pct4emoji ! PercentFound(trdac.count)
        pops4emoji ! PopularityFound(trdac.trda.trd.emos)
      }
      if (trdac.trda.trd.urls.nonEmpty) {
        // logger.info(s"urls.nonEmpty at ${trdac.toString}")
        pct4url ! PercentFound(trdac.count)
        pops4tldomain ! PopularityFound(trdac.trda.trd.urls)
      }
      if (trdac.trda.trd.purls.nonEmpty) {
        // logger.info(s"purls.nonEmpty at ${trdac.toString}")
        pct4purl ! PercentFound(trdac.count)
      }
      if (trdac.trda.trd.hts.nonEmpty) {
        // logger.info(s"hts.nonEmpty at ${trdac.toString}")
        pops4hashtag ! PopularityFound(trdac.trda.trd.hts)
      }
      trdac
    })
}
