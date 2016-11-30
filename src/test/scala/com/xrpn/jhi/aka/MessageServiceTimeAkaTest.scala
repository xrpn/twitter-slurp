package com.xrpn.jhi.aka

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestActorRef, TestKit}
import com.typesafe.config.ConfigFactory
import com.xrpn.jhi.aka.StatsCollectorAka.{JsonServiceCount, JsonServiceTime}
import com.xrpn.jhi.util.MeanServiceTime
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuiteLike}

/**
  * Created by alsq on 11/23/16.
  */
class MessageServiceTimeAkaTest (_system: ActorSystem) extends TestKit(_system)
  with ImplicitSender
  with FunSuiteLike
  with BeforeAndAfterEach
  with BeforeAndAfterAll {

  import MeanServiceTime._
  import MessageServiceTimeAka._

  def this() = this(ActorSystem("test-system", ConfigFactory.parseString(
    """akka {
          loggers = ["akka.testkit.TestEventListener"]
          loglevel = "WARNING"
       }""")))

  val autActor = TestActorRef[MessageServiceTimeAka]
  val startOfTime = System.currentTimeMillis
  val betweenTime = 20

  test("smoke") {
    assert(1 === 1)
    assert(1 !== Option(1))
  }

  test("service time, cardinality on empty") {
    autActor ! CardinalityInquiry
    val na = expectMsgType[JsonServiceCount]
    assert(0 === na.result.split(MessageServiceTimeAka.resultBreak)(1).toInt)
    autActor ! ServiceTimeInquiry
    val st = expectMsgType[JsonServiceTime]
    assert(unavailable === st.result.split(MessageServiceTimeAka.resultBreak)(1).toDouble)
  }

  test("service time, cardinality after new epoch") {
    autActor ! NewEpoch
    autActor ! CardinalityInquiry
    val na = expectMsgType[JsonServiceCount]
    assert(0 === na.result.split(MessageServiceTimeAka.resultBreak)(1).toInt)
    autActor ! ServiceTimeInquiry
    val st = expectMsgType[JsonServiceTime]
    assert(unavailable === st.result.split(MessageServiceTimeAka.resultBreak)(1).toDouble)
  }

  test("service time, cardinality on one") {
    autActor ! NewEpoch
    autActor ! RawJsonServiced(startOfTime)
    autActor ! CardinalityInquiry
    val na = expectMsgType[JsonServiceCount]
    assert(1 === na.result.split(MessageServiceTimeAka.resultBreak)(1).toInt)
    autActor ! ServiceTimeInquiry
    val st = expectMsgType[JsonServiceTime]
    assert(insufficientArrivals === st.result.split(MessageServiceTimeAka.resultBreak)(1).toDouble)
  }

  test("service time, cardinality on two") {
    autActor ! NewEpoch
    autActor ! RawJsonServiced(startOfTime)
    autActor ! RawJsonServiced(startOfTime+betweenTime)
    autActor ! CardinalityInquiry
    val na = expectMsgType[JsonServiceCount]
    assert(2 === na.result.split(MessageServiceTimeAka.resultBreak)(1).toInt)
    autActor ! ServiceTimeInquiry
    val st = expectMsgType[JsonServiceTime]
    assert((betweenTime/2).toDouble === st.result.split(MessageServiceTimeAka.resultBreak)(1).toDouble)
  }
}

