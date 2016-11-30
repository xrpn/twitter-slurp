package com.xrpn.jhi.aka

import akka.actor.{Actor, Props}
import akka.event.Logging
import com.xrpn.jhi.aka.ReaperAka.WatchMe
import com.xrpn.jhi.aka.StatsCollectorAka.{Percent, PercentCount}
import com.xrpn.jhi.util.PercentOfTotal

object PercentAka {

  sealed trait PercentAKA
  case class PercentFound(totalTweets: Int) extends PercentAKA
  case object CardinalityInquiry extends PercentAKA
  case object PercentInquiry extends PercentAKA
  case object NewEpoch extends PercentAKA
  case object Ping extends PercentAKA

  def props1(src:  StatsActors.StatSource) = Props(classOf[PercentAka],src)
  val defaultActorName = "percent-aka"
  val resultBreak = "==>>"
}

/**
  * Intended to tally a running percent of tweets from StatsSource.  Incidentally,
  * this can also be adapted as an event counter.
  * Created by alsq on 11/23/16.
  */
class PercentAka(src: StatsActors.StatSource) extends Actor {

  import PercentAka._
  import PercentOfTotal._
  import context._

  val tag = StatsActors.statsSrcAkaNames(src)
  val log =  Logging(context.system.eventStream, tag)

  StatsActors.getStatsReaper ! WatchMe(self)

  override def receive: Receive = activatedWith(emptyPercentRecord)

  def activatedWith(current: PercentRecord): Receive = {
    case PercentFound(totSoFar) =>
      // messages may arrive out of order, so keep the max of total
      become(activatedWith(PercentRecord(current.count+1,Math.max(totSoFar,current.total))))
    case PercentInquiry =>
      val st = computePercent(current)
      sender ! Percent(f"${tag}${resultBreak}$st%.2f")
    case CardinalityInquiry =>
      sender ! PercentCount(s"${tag}${resultBreak}${current.count}")
    case NewEpoch => become(activatedWith(emptyPercentRecord))
    case Ping => log.info(s"alive with ${current.toString}")
  }
}
