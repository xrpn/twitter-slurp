package com.xrpn.jhi.aka

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestActorRef, TestKit}
import com.typesafe.config.ConfigFactory
import com.xrpn.jhi.aka.StatsCollectorAka.{Percent, PercentCount}
import com.xrpn.jhi.util.PercentOfTotal
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuiteLike}

/**
  * Created by alsq on 11/23/16.
  */
class PercentAkaTest  (_system: ActorSystem) extends TestKit(_system)
  with ImplicitSender
  with FunSuiteLike
  with BeforeAndAfterEach
  with BeforeAndAfterAll {

  import PercentAka._
  import PercentOfTotal._

  def this() = this(ActorSystem("test-system", ConfigFactory.parseString(
    """akka {
          loggers = ["akka.testkit.TestEventListener"]
          loglevel = "WARNING"
       }""")))

  val src = StatsActors.PercentEmoji
  val break = PercentAka.resultBreak
  val srcName = StatsActors.statsSrcAkaNames(src)
  val autActor = TestActorRef(new PercentAka(src))

  def resultHelper(res: String): Option[String] = {
    val aux: Array[String] = res.split(break)
    assert(aux(0) === srcName)
    if (2 == aux.length) Option(aux(1)) else None
  }

  test("smoke") {
    assert(1 === 1)
    assert(1 !== Option(1))
  }

  test("percent, cardinality on empty") {
    autActor ! CardinalityInquiry
    val na = expectMsgType[PercentCount]
    val nar = resultHelper(na.result)
    assert(0 === nar.get.toInt)
    autActor ! PercentInquiry
    val st = expectMsgType[Percent]
    val res = resultHelper(st.result)
    assert(unavailable === res.get.toDouble)
  }

  test("percent, cardinality oo new epoch") {
    autActor ! NewEpoch
    autActor ! CardinalityInquiry
    val na = expectMsgType[PercentCount]
    val nar = resultHelper(na.result)
    assert(0 === nar.get.toInt)
    autActor ! PercentInquiry
    val st = expectMsgType[Percent]
    val res = resultHelper(st.result)
    assert(unavailable === res.get.toDouble)
  }

  test("percent, cardinality on one") {
    autActor ! NewEpoch
    autActor ! PercentFound(4)
    autActor ! CardinalityInquiry
    val na = expectMsgType[PercentCount]
    val nar = resultHelper(na.result)
    assert(1 === nar.get.toInt)
    autActor ! PercentInquiry
    val st = expectMsgType[Percent]
    val res = resultHelper(st.result)
    assert(25.0 === res.get.toDouble)
  }

  test("percent, cardinality on two") {
    autActor ! NewEpoch
    autActor ! PercentFound(4)
    autActor ! PercentFound(16)
    autActor ! CardinalityInquiry
    val na = expectMsgType[PercentCount]
    val nar = resultHelper(na.result)
    assert(2 === nar.get.toInt)
    autActor ! PercentInquiry
    val st = expectMsgType[Percent]
    val res = resultHelper(st.result)
    assert(12.5 === res.get.toDouble)
  }
}
