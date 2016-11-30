package com.xrpn.jhi.aka

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestActorRef, TestKit}
import com.typesafe.config.ConfigFactory
import com.xrpn.jhi.aka.StatsCollectorAka.{TweetArrivalCount, TweetArrivalInterval}
import com.xrpn.jhi.util.MeanServiceTime
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuiteLike}

/**
  * Created by alsq on 11/23/16.
  */
class TwitterArrivalTimeAkaTest (_system: ActorSystem) extends TestKit(_system)
  with ImplicitSender
  with FunSuiteLike
  with BeforeAndAfterEach
  with BeforeAndAfterAll {

  import MeanServiceTime._
  import TweetArrivalIntervalAka._

  def this() = this(ActorSystem("test-system", ConfigFactory.parseString(
    """akka {
          loggers = ["akka.testkit.TestEventListener"]
          loglevel = "WARNING"
       }""")))

  val autActor = TestActorRef[TweetArrivalIntervalAka]
  val startOfTime = System.currentTimeMillis
  val betweenTime = 20

  test("smoke") {
    assert(1 === 1)
    assert(1 !== Option(1))
  }

  test("service time on empty") {
    autActor ! CardinalityInquiry
    val na = expectMsgType[TweetArrivalCount]
    assert(0 === na.result.split(TweetArrivalIntervalAka.resultBreak)(1).toInt)
    autActor ! ArrivalIntervalInquiry
    val st = expectMsgType[TweetArrivalInterval]
    assert(unavailable === st.result.split(TweetArrivalIntervalAka.resultBreak)(1).toDouble)
  }

  test("service time after new epoch") {
    autActor ! NewEpoch
    autActor ! CardinalityInquiry
    val na = expectMsgType[TweetArrivalCount]
    assert(0 === na.result.split(TweetArrivalIntervalAka.resultBreak)(1).toInt)
    autActor ! ArrivalIntervalInquiry
    val st = expectMsgType[TweetArrivalInterval]
    assert(unavailable === st.result.split(TweetArrivalIntervalAka.resultBreak)(1).toDouble)
  }

  test("service time, cardinality on one") {
    autActor ! NewEpoch
    autActor ! TweetArrival(startOfTime)
    autActor ! CardinalityInquiry
    val na = expectMsgType[TweetArrivalCount]
    assert(1 === na.result.split(TweetArrivalIntervalAka.resultBreak)(1).toInt)
    autActor ! ArrivalIntervalInquiry
    val st = expectMsgType[TweetArrivalInterval]
    assert(insufficientArrivals === st.result.split(TweetArrivalIntervalAka.resultBreak)(1).toDouble)
  }

  test("service time, cardinality on two") {
    autActor ! NewEpoch
    autActor ! TweetArrival(startOfTime)
    autActor ! TweetArrival(startOfTime+betweenTime)
    autActor ! CardinalityInquiry
    val na = expectMsgType[TweetArrivalCount]
    assert(2 === na.result.split(TweetArrivalIntervalAka.resultBreak)(1).toInt)
    autActor ! ArrivalIntervalInquiry
    val st = expectMsgType[TweetArrivalInterval]
    assert((betweenTime/2).toDouble === st.result.split(TweetArrivalIntervalAka.resultBreak)(1).toDouble)
  }

  test("secondly service time, cardinality on two") {
    autActor ! NewEpoch
    // two arrival in less than one second
    autActor ! TweetArrival(startOfTime)
    autActor ! TweetArrival(startOfTime+betweenTime)
    autActor ! CardinalityInquiry
    val na = expectMsgType[TweetArrivalCount]
    assert(2 === na.result.split(TweetArrivalIntervalAka.resultBreak)(1).toInt)
    autActor ! SecondlyArrivalIntervalInquiry
    val st = expectMsgType[TweetArrivalInterval]
    // unavailable because less than a second
    assert(unavailable === st.result.split(TweetArrivalIntervalAka.resultBreak)(1).toDouble)
  }

  test("secondly service time, cardinality on three") {
    autActor ! NewEpoch
    // two arrivals during the first second
    autActor ! TweetArrival(startOfTime)
    autActor ! TweetArrival(startOfTime+betweenTime)
    // another arrival past one second
    autActor ! TweetArrival(startOfTime+betweenTime+msInSecond)
    autActor ! CardinalityInquiry
    val na = expectMsgType[TweetArrivalCount]
    assert(3 === na.result.split(TweetArrivalIntervalAka.resultBreak)(1).toInt)
    autActor ! SecondlyArrivalIntervalInquiry
    val sts = expectMsgType[TweetArrivalInterval]
    assert((msInSecond/2).toDouble === sts.result.split(TweetArrivalIntervalAka.resultBreak)(1).toDouble)
    autActor ! MinutelyArrivalIntervalInquiry
    // nothing past minute or hour
    val stm = expectMsgType[TweetArrivalInterval]
    assert(unavailable === stm.result.split(TweetArrivalIntervalAka.resultBreak)(1).toDouble)
    autActor ! HourlyArrivalIntervalInquiry
    val sth = expectMsgType[TweetArrivalInterval]
    assert(unavailable === sth.result.split(TweetArrivalIntervalAka.resultBreak)(1).toDouble)
  }

  test("minutely service time, cardinality on two") {
    autActor ! NewEpoch
    // two arrivals within less than a minute
    autActor ! TweetArrival(startOfTime)
    autActor ! TweetArrival(startOfTime+betweenTime+msInSecond)
    autActor ! CardinalityInquiry
    val na = expectMsgType[TweetArrivalCount]
    assert(2 === na.result.split(TweetArrivalIntervalAka.resultBreak)(1).toInt)
    autActor ! MinutelyArrivalIntervalInquiry
    val st = expectMsgType[TweetArrivalInterval]
    // unavailable because less than one minute
    assert(unavailable === st.result.split(TweetArrivalIntervalAka.resultBreak)(1).toDouble)
  }

  test("minutely service time, cardinality on three") {
    autActor ! NewEpoch
    // two arrivals within less than a minute, but past a second
    autActor ! TweetArrival(startOfTime) // (1)
    autActor ! TweetArrival(startOfTime+betweenTime+msInSecond) // (2)
    // one arrival past a minute
    autActor ! TweetArrival(startOfTime+betweenTime+msInMinute) // (3)
    autActor ! CardinalityInquiry
    val na = expectMsgType[TweetArrivalCount]
    assert(3 === na.result.split(TweetArrivalIntervalAka.resultBreak)(1).toInt)
    autActor ! SecondlyArrivalIntervalInquiry
    val sts = expectMsgType[TweetArrivalInterval]
    // (3) arrived 1 min past (2), therefore the last second before (3) has no arrivals
    assert(insufficientArrivals === sts.result.split(TweetArrivalIntervalAka.resultBreak)(1).toDouble)
    autActor ! MinutelyArrivalIntervalInquiry
    val stm = expectMsgType[TweetArrivalInterval]
    assert((msInMinute/2).toDouble === stm.result.split(TweetArrivalIntervalAka.resultBreak)(1).toDouble)
    autActor ! HourlyArrivalIntervalInquiry
    val sth = expectMsgType[TweetArrivalInterval]
    assert(unavailable === sth.result.split(TweetArrivalIntervalAka.resultBreak)(1).toDouble)
  }

  test("hourly service time, cardinality on two") {
    autActor ! NewEpoch
    autActor ! TweetArrival(startOfTime)
    autActor ! TweetArrival(startOfTime+betweenTime+msInMinute)
    autActor ! CardinalityInquiry
    val na = expectMsgType[TweetArrivalCount]
    assert(2 === na.result.split(TweetArrivalIntervalAka.resultBreak)(1).toInt)
    autActor ! HourlyArrivalIntervalInquiry
    val st = expectMsgType[TweetArrivalInterval]
    assert(unavailable === st.result.split(TweetArrivalIntervalAka.resultBreak)(1).toDouble)
  }

  test("hourly service time, cardinality on three") {
    autActor ! NewEpoch
    autActor ! TweetArrival(startOfTime)
    autActor ! TweetArrival(startOfTime+betweenTime+msInMinute)
    autActor ! TweetArrival(startOfTime+betweenTime+msInHour)
    autActor ! CardinalityInquiry
    val na = expectMsgType[TweetArrivalCount]
    assert(3 === na.result.split(TweetArrivalIntervalAka.resultBreak)(1).toInt)
    autActor ! SecondlyArrivalIntervalInquiry
    val sts = expectMsgType[TweetArrivalInterval]
    assert(insufficientArrivals === sts.result.split(TweetArrivalIntervalAka.resultBreak)(1).toDouble)
    autActor ! MinutelyArrivalIntervalInquiry
    val stm = expectMsgType[TweetArrivalInterval]
    assert(insufficientArrivals === stm.result.split(TweetArrivalIntervalAka.resultBreak)(1).toDouble)
    autActor ! HourlyArrivalIntervalInquiry
    val sth = expectMsgType[TweetArrivalInterval]
    assert((msInHour/2).toDouble === sth.result.split(TweetArrivalIntervalAka.resultBreak)(1).toDouble)
  }
}

