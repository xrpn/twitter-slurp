package com.xrpn.jhi.aka

import akka.actor.{Actor, Props}
import akka.event.Logging
import com.xrpn.jhi.aka.ReaperAka.WatchMe
import com.xrpn.jhi.aka.StatsCollectorAka.{JsonServiceCount, JsonServiceTime}
import com.xrpn.jhi.util.MeanServiceTime

object MessageServiceTimeAka {

  sealed trait RawJsonMST
  case class RawJsonServiced(aging: Long) extends RawJsonMST
  case object ServiceTimeInquiry extends RawJsonMST
  case object CardinalityInquiry extends RawJsonMST
  case object NewEpoch extends RawJsonMST
  case object Ping extends RawJsonMST

  val props1 = Props[MessageServiceTimeAka]
  val defaultActorName = "message-service-time"
  val resultBreak = "==>>"

}

/**
  * Intended to tally the mean service time for raw Json messages, i.e. the rate
  * at which the raw buffer is emptied.  This is needed to compare with mean arrival
  * time into the buffer and check buffering health according to Little's law.
  * Created by alsq on 11/22/16.
  */
class MessageServiceTimeAka extends Actor {

  import MeanServiceTime._
  import MessageServiceTimeAka._
  import context._

  val tag = defaultActorName
  val log =  Logging(context.system.eventStream, tag)

  StatsActors.getStatsReaper ! WatchMe(self)

  override def receive: Receive = activatedWith(emptyAgingRecord)

  /**
    * Workhorse, stateful between invocations using "become".
    * @param current
    * @return
    */
  def activatedWith(current: AgingRecord): Receive = {
    if (emptyAgingRecord.equals(current)) {
      case RawJsonServiced(aging) =>
        become(activatedWith(updateAgingRecord(aging, current)))
      case ServiceTimeInquiry =>
        sender ! JsonServiceTime(f"$tag$resultBreak$unavailable%.2f")
      case CardinalityInquiry =>
        sender ! JsonServiceCount(s"$tag$resultBreak" + "0")
      case NewEpoch => become(activatedWith(emptyAgingRecord))
      case Ping => log.info("alive with empty aging record")
    } else {
      case RawJsonServiced(aging) =>
        become(activatedWith(updateAgingRecord(aging, current)))
      case ServiceTimeInquiry =>
        val st = computeMeanServiceTime(current)
        sender ! JsonServiceTime(f"$tag$resultBreak$st%.2f")
      case CardinalityInquiry =>
        sender ! JsonServiceCount(s"$tag$resultBreak${current.arrivals}")
      case NewEpoch => become(activatedWith(emptyAgingRecord))
      case Ping => log.info(s"alive with ${current.toString}")
    }
  }
}
