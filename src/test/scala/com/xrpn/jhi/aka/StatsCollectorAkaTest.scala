package com.xrpn.jhi.aka

import akka.actor.{ActorSelection, ActorSystem, PoisonPill}
import akka.testkit.{ImplicitSender, TestActorRef, TestKit}
import com.typesafe.config.ConfigFactory
import com.xrpn.jhi.aka.MessageServiceTimeAka.RawJsonServiced
import com.xrpn.jhi.aka.PercentAka.PercentFound
import com.xrpn.jhi.aka.PopularityAka.PopularityFound
import com.xrpn.jhi.aka.StatsActors.{MessageServiceTime, PercentEmoji, PercentPhotourl, PercentUrl, StatSource, TweetArrivalInterval, TweetCounter}
import com.xrpn.jhi.aka.StatsCollectorAka.{CollectMeanTime, CollectPercent, CollectPops4Emoji, CollectPops4HashTag, CollectPops4Tld, CollectTweetCount}
import com.xrpn.jhi.aka.TweetArrivalIntervalAka.TweetArrival
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuiteLike}

/**
  * Created by alsq on 11/23/16.
  */
object StatsCollectorAkaTest {

  def countResultCanBeCorrect(res: String) = {
    res.nonEmpty &&
      res.contains(StatsActors.statsSrcAkaNames(TweetCounter)) &&
      res.split('\n').toList.map(msg => {
        msg.split(PercentAka.resultBreak)(1).nonEmpty
      }).forall(p => p)
  }

  def percentResultCanBeCorrect(res: String) = {
    res.nonEmpty &&
      res.contains(StatsActors.statsSrcAkaNames(PercentEmoji)) &&
      res.contains(StatsActors.statsSrcAkaNames(PercentPhotourl)) &&
      res.contains(StatsActors.statsSrcAkaNames(PercentUrl)) &&
      res.split('\n').toList.map(msg => {
        msg.split(PercentAka.resultBreak)(1).nonEmpty
      }).forall(p => p)
  }

  def meantimeResultCanBeCorrect(res: String) = {
    res.nonEmpty &&
      res.contains(StatsActors.statsSrcAkaNames(MessageServiceTime)) &&
      res.contains(StatsActors.statsSrcAkaNames(TweetArrivalInterval)) &&
      MessageServiceTimeAka.resultBreak.equals(TweetArrivalIntervalAka.resultBreak) &&
      res.split('\n').toList.map(msg => {
        msg.split(TweetArrivalIntervalAka.resultBreak)(1).nonEmpty
      }).forall(p => p)
  }

  def popsResultCanBeCorrect(res: String, statSource: StatSource): Boolean = {
    res.nonEmpty &&
      res.contains(StatsActors.statsSrcAkaNames(statSource)) &&
      res.split('\n').toList.map(msg => {
        msg.split(PopularityAka.resultBreak)(1).contains(PopularityAka.spltrStr)
      }).forall(p => p)
  }

}

class StatsCollectorAkaTest  (_system: ActorSystem) extends TestKit(_system)
  with ImplicitSender
  with FunSuiteLike
  with BeforeAndAfterEach
  with BeforeAndAfterAll {

  def this() = this(ActorSystem("test-system", ConfigFactory.parseString(
    """akka {
          loggers = ["akka.testkit.TestEventListener"]
          loglevel = "WARNING"
       }""")))

  val autActor = TestActorRef[StatsCollectorAka]
  val isPrintingAssert = true

  import StatsActors._
  import StatsCollectorAkaTest._

  // cannot call till after beforeAll where initialization happens
  private lazy val mst4rawjsonAka: ActorSelection = getStatsActorByName(MessageServiceTimeAka.defaultActorName)
  private lazy val tai4tweetAka: ActorSelection = getStatsActorByName(TweetArrivalIntervalAka.defaultActorName)
  private lazy val counter4tweet: ActorSelection = getStatsActorByName(countOfTweetsAkaName)
  private lazy val pct4emoji: ActorSelection = getStatsActorByName(percentEmojiAkaName)
  private lazy val pct4url: ActorSelection = getStatsActorByName(percentUrlAkaName)
  private lazy val pct4purl: ActorSelection = getStatsActorByName(percentPhotourlAkaName)
  private lazy val pops4emoji: ActorSelection = getStatsActorByName(popularityEmojiAkaName)
  private lazy val pops4hashtag: ActorSelection = getStatsActorByName(popularityHastagAkaName)
  private lazy val pops4tldomain: ActorSelection = getStatsActorByName(popularityTldomainAkaName)


  override def beforeAll(): Unit = {
    initAllActors
    Thread.sleep(1000)
  }

  override def afterAll(): Unit = {
    getAllStatsActors.foreach( actor => actor ! PoisonPill )
  }

  override def beforeEach(): Unit = {
    val timeStamp = System.currentTimeMillis()
    mst4rawjsonAka ! RawJsonServiced(timeStamp)
    tai4tweetAka ! TweetArrival(timeStamp)
    counter4tweet ! PercentFound(1)
    pct4emoji ! PercentFound(20)
    pct4url ! PercentFound(5)
    pct4purl ! PercentFound(10)
    pops4emoji ! PopularityFound(List(":smile:"))
    pops4hashtag ! PopularityFound(List("myFaves"))
    pops4tldomain ! PopularityFound(List("http://www.google.com"))
  }

  test("smoke") {
    assert(1 === 1)
    assert(1 !== Option(1))
  }

  test("collect percent") {
    autActor ! CollectPercent
    val res = expectMsgType[String]
    assert(percentResultCanBeCorrect(res))
    if (isPrintingAssert) println(res)
  }

  test("collect count") {
    autActor ! CollectTweetCount
    val res = expectMsgType[String]
    assert(countResultCanBeCorrect(res))
    if (isPrintingAssert) println(res)
  }

  test("collect emoji pops") {
    val timeStamp = System.currentTimeMillis()
    autActor ! CollectPops4Emoji
    val res = expectMsgType[String]
    assert(popsResultCanBeCorrect(res,PopularityEmoji))
    if (isPrintingAssert) println(res)
  }

  test("collect hashtag pops") {
    val timeStamp = System.currentTimeMillis()
    autActor ! CollectPops4HashTag
    val res = expectMsgType[String]
    assert(popsResultCanBeCorrect(res,PopularityHashtag))
    if (isPrintingAssert) println(res)
  }

  test("collect tld pops") {
    val timeStamp = System.currentTimeMillis()
    autActor ! CollectPops4Tld
    val res = expectMsgType[String]
    assert(popsResultCanBeCorrect(res,PopularityTldomain))
    if (isPrintingAssert) println(res)
  }

  test("collect mean time") {
    val timeStamp = System.currentTimeMillis()
    autActor ! CollectMeanTime
    val res = expectMsgType[String]
    assert(meantimeResultCanBeCorrect(res))
    if (isPrintingAssert) println(res)
  }

}
