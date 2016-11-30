package com.xrpn.jhi.aka

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestActorRef, TestKit}
import com.typesafe.config.ConfigFactory
import com.xrpn.jhi.aka.StatsCollectorAka.{Popularity, PopularityCardinality}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuiteLike}

/**
  * Created by alsq on 11/23/16.
  */
class PopularityAkaTest (_system: ActorSystem) extends TestKit(_system)
  with ImplicitSender
  with FunSuiteLike
  with BeforeAndAfterEach
  with BeforeAndAfterAll {

  import PopularityAka._

  def this() = this(ActorSystem("test-system", ConfigFactory.parseString(
    """akka {
          loggers = ["akka.testkit.TestEventListener"]
          loglevel = "WARNING"
       }""")))

  val src = StatsActors.PopularityEmoji
  val break = PopularityAka.resultBreak
  val srcName = StatsActors.statsSrcAkaNames(src)
  val autActor = TestActorRef(new PopularityAka(src))

  val msg1 = "afsdfsadfe"
  val msg2 = s"1$msg1 /*()&^%2345"
  val resMsg1_1 = Set(s"$msg1${PopularityAka.spltrStr}1")
  val resMsg1_2 = Set(s"$msg1${PopularityAka.spltrStr}2")
  val resMsg1_1$Msg2_1 = Set(s"$msg1${PopularityAka.spltrStr}1",s"$msg2${PopularityAka.spltrStr}1")
  val resMsg1_1$Msg2_2 = Set(s"$msg1${PopularityAka.spltrStr}1",s"$msg2${PopularityAka.spltrStr}2")

  def resultHelper(res: String): Option[String] = {
    val aux: Array[String] = res.split(break)
    assert(aux(0) === srcName)
    if (2 == aux.length) Option(aux(1)) else None
  }

  test("smoke") {
    assert(1 === 1)
    assert(1 !== Option(1))
  }

  test("popularity, cardinality on empty") {
    autActor ! TopsCardinalityInquiry
    val na = expectMsgType[PopularityCardinality]
    val nar = resultHelper(na.result)
    assert(0 === nar.get.toInt)
    autActor ! PopularityInquiry
    val st = expectMsgType[Popularity]
    val res = resultHelper(st.result)
    assert(Some(onEmptyRes) === res)
  }

  test("popularity, cardinality oo new epoch") {
    autActor ! NewEpoch
    autActor ! TopsCardinalityInquiry
    val na = expectMsgType[PopularityCardinality]
    val nar = resultHelper(na.result)
    assert(0 === nar.get.toInt)
    autActor ! PopularityInquiry
    val st = expectMsgType[Popularity]
    val res = resultHelper(st.result)
    assert(Some(onEmptyRes) === res)
  }

  test("popularity, cardinality on one") {
    autActor ! NewEpoch
    autActor ! PopularityFound(List(msg1))
    autActor ! TopsCardinalityInquiry
    val na = expectMsgType[PopularityCardinality]
    val nar = resultHelper(na.result)
    assert(1 === nar.get.toInt)
    autActor ! PopularityInquiry
    val st = expectMsgType[Popularity]
    val res = resultHelper(st.result)
    assert(res.get === resMsg1_1.iterator.next)
  }

  test("popularity, cardinality on distinct two, distinct found") {
    autActor ! NewEpoch
    autActor ! PopularityFound(List(msg1))
    Thread.sleep(20)
    autActor ! PopularityFound(List(msg2))
    autActor ! TopsCardinalityInquiry
    val na = expectMsgType[PopularityCardinality]
    val nar = resultHelper(na.result)
    assert(2 === nar.get.toInt)
    autActor ! PopularityInquiry
    val st = expectMsgType[Popularity]
    val res = resultHelper(st.result)
    assert(res.get.split(";").toSet === resMsg1_1$Msg2_1)
  }

  test("popularity, cardinality on distinct two, same found") {
    autActor ! NewEpoch
    autActor ! PopularityFound(List(msg1,msg2))
    autActor ! TopsCardinalityInquiry
    val na = expectMsgType[PopularityCardinality]
    val nar = resultHelper(na.result)
    assert(2 === nar.get.toInt)
    autActor ! PopularityInquiry
    val st = expectMsgType[Popularity]
    val res = resultHelper(st.result)
    assert(res.get.split(";").toSet === resMsg1_1$Msg2_1)
  }

  test("popularity, cardinality on same two, distinct found") {
    autActor ! NewEpoch
    autActor ! PopularityFound(List(msg1))
    Thread.sleep(20)
    autActor ! PopularityFound(List(msg1))
    autActor ! TopsCardinalityInquiry
    val na = expectMsgType[PopularityCardinality]
    val nar = resultHelper(na.result)
    assert(1 === nar.get.toInt)
    autActor ! PopularityInquiry
    val st = expectMsgType[Popularity]
    val res = resultHelper(st.result)
    assert(res.get === resMsg1_2.iterator.next)
  }

  test("popularity, cardinality on same two, same found") {
    autActor ! NewEpoch
    // this is a duplicate vote, must be counted as one
    autActor ! PopularityFound(List(msg1,msg1))
    autActor ! TopsCardinalityInquiry
    val na = expectMsgType[PopularityCardinality]
    val nar = resultHelper(na.result)
    assert(1 === nar.get.toInt)
    autActor ! PopularityInquiry
    val st = expectMsgType[Popularity]
    val res = resultHelper(st.result)
    assert(res.get === resMsg1_1.iterator.next)
  }

  test("popularity, cardinality on multiple, distinct found") {
    autActor ! NewEpoch
    autActor ! PopularityFound(List(msg1, msg2))
    Thread.sleep(20)
    autActor ! PopularityFound(List(msg2))
    autActor ! TopsCardinalityInquiry
    val na = expectMsgType[PopularityCardinality]
    val nar = resultHelper(na.result)
    assert(2 === nar.get.toInt)
    autActor ! PopularityInquiry
    val st = expectMsgType[Popularity]
    val res = resultHelper(st.result)
    assert(res.get.split(";").toSet === resMsg1_1$Msg2_2)
  }
}
