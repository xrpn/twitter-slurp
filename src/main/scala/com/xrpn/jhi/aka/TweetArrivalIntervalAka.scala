package com.xrpn.jhi.aka

import akka.actor.{Actor, Props}
import akka.event.Logging
import com.xrpn.jhi.aka.ReaperAka.WatchMe
import com.xrpn.jhi.aka.StatsCollectorAka.{TweetArrivalCount, TweetArrivalInterval}
import com.xrpn.jhi.util.MeanServiceTime

object TweetArrivalIntervalAka {

  sealed trait TweetMST
  case class TweetArrival(aging: Long) extends TweetMST
  case object ArrivalIntervalInquiry extends TweetMST
  case object HourlyArrivalIntervalInquiry extends TweetMST
  case object MinutelyArrivalIntervalInquiry extends TweetMST
  case object SecondlyArrivalIntervalInquiry extends TweetMST
  case object AllPeriodArrivalIntervalInquiry extends TweetMST
  case object CardinalityInquiry extends TweetMST
  case object NewEpoch extends TweetMST
  case object Ping extends TweetMST

  val props1 = Props[TweetArrivalIntervalAka]
  val defaultActorName = "tweet-arrival-interval"
  val resultBreak = "==>>"

}

/**
  * In fulfillment of the requirement to compute mean arrival interval by
  * hour, minute, or second.  Also computes mean arrival interval from the
  * beginning of service.
  * Created by alsq on 11/22/16.
  */
class TweetArrivalIntervalAka extends Actor {

  import MeanServiceTime._
  import TweetArrivalIntervalAka._
  import context._

  val tag = defaultActorName
  val log =  Logging(context.system.eventStream, tag)

  StatsActors.getStatsReaper ! WatchMe(self)

  override def receive: Receive = activatedWith(emptyAgingRecord,List(),List(),List())

  /**
    * Workhorse to keep state between invocations using "become".
    * @param current the present aging record.  Used to compute means from epoch (i.e. the beginning of time).
    * @param har state for hourly arrivals.
    * @param mar state for arrivals by minute.
    * @param sar state for arrivals by second.
    */
  def activatedWith(current: AgingRecord, har:List[AgingRecord], mar:List[AgingRecord], sar:List[AgingRecord] ): Receive = {
    if (emptyAgingRecord.equals(current)) {
      case TweetArrival(aging) =>
        val new_ar = updateAgingRecord(aging, current)
        val new_har = updateHourlyServiceTime(aging, har)
        val new_mar = updateMinutelyServiceTime(aging, mar)
        val new_sar = updateSecondlyServiceTime(aging, sar)
        become(activatedWith(new_ar, new_har, new_mar, new_sar))
      case ArrivalIntervalInquiry =>
        sender ! TweetArrivalInterval(f"$tag$resultBreak$unavailable%.2f")
      case HourlyArrivalIntervalInquiry =>
        sender ! TweetArrivalInterval(f"( H )$tag$resultBreak$unavailable%.2f")
      case MinutelyArrivalIntervalInquiry =>
        sender ! TweetArrivalInterval(f"( m )$tag$resultBreak$unavailable%.2f")
      case SecondlyArrivalIntervalInquiry =>
        sender ! TweetArrivalInterval(f"( s )$tag$resultBreak$unavailable%.2f")
      case AllPeriodArrivalIntervalInquiry =>
        sender ! TweetArrivalInterval(f"( H/m/s/# )$tag$resultBreak$unavailable%.2f/$unavailable%.2f/$unavailable%.2f/$unavailable%.2f")
      case CardinalityInquiry =>
        sender ! TweetArrivalCount(s"$tag$resultBreak" + "0")
      case NewEpoch => become(activatedWith(emptyAgingRecord, List(), List(), List()))
      case Ping => log.info("alive with empty state")
    } else {
      case TweetArrival(aging) =>
        val new_ar: AgingRecord = updateAgingRecord(aging, current)
        val new_har: List[AgingRecord] = updateHourlyServiceTime(aging, har)
        val new_mar: List[AgingRecord] = updateMinutelyServiceTime(aging, mar)
        val new_sar: List[AgingRecord] = updateSecondlyServiceTime(aging, sar)
        become(activatedWith(new_ar, new_har, new_mar, new_sar))
      case ArrivalIntervalInquiry =>
        val ati = computeMeanServiceTime(current)
        sender ! TweetArrivalInterval(f"$tag$resultBreak$ati%.2f")
      case HourlyArrivalIntervalInquiry =>
        val hati = computeMeanServiceTime(har)
        sender ! TweetArrivalInterval(f"( H )$tag$resultBreak$hati%.2f")
      case MinutelyArrivalIntervalInquiry =>
        val mati = computeMeanServiceTime(mar)
        sender ! TweetArrivalInterval(f"( m )$tag$resultBreak$mati%.2f")
      case SecondlyArrivalIntervalInquiry =>
        val sati = computeMeanServiceTime(sar)
        sender ! TweetArrivalInterval(f"( s )$tag$resultBreak$sati%.2f")
      case AllPeriodArrivalIntervalInquiry =>
        val ati = computeMeanServiceTime(current)
        val sati = computeMeanServiceTime(sar)
        val mati = computeMeanServiceTime(mar)
        val hati = computeMeanServiceTime(har)
        sender ! TweetArrivalInterval(f"( H/m/s/# )$tag$resultBreak$hati%.2f/$mati%.2f/$sati%.2f/$ati%.2f")
      case CardinalityInquiry =>
        sender ! TweetArrivalCount(s"$tag$resultBreak${current.arrivals}")
      case NewEpoch => become(activatedWith(emptyAgingRecord, List(), List(), List()))
      case Ping => log.info("alive with non-empty state")
    }
  }
}
