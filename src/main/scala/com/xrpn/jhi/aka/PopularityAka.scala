package com.xrpn.jhi.aka

import akka.actor.{Actor, Props}
import akka.event.Logging
import com.xrpn.jhi.aka.ReaperAka.WatchMe
import com.xrpn.jhi.aka.StatsCollectorAka.{Popularity, PopularityCardinality}
import com.xrpn.jhi.util.MostPopular

object PopularityAka {

  sealed trait PopularityAKA
  case class PopularityFound(items: List[String]) extends PopularityAKA
  case object TopsCardinalityInquiry extends PopularityAKA
  case object PopularityInquiry extends PopularityAKA
  case object NewEpoch extends PopularityAKA
  case object Ping extends PopularityAKA

  def props1(src: StatsActors.StatSource) = Props(classOf[PopularityAka],src)
  val defaultActorName = "popularity-aka"
  val resultBreak = "==>>"
  val spltr = '\t'
  val spltrStr = spltr.toString
  val onEmptyRes = "EMPTY"
}

/**
  * Intended to tally the popularity of certain tweet items from StatsSource.
  * Created by alsq on 11/23/16.
  */
class PopularityAka(src:  StatsActors.StatSource) extends Actor {

  import MostPopular._
  import PopularityAka._
  import context._

  val tag = StatsActors.statsSrcAkaNames(src)
  val log =  Logging(context.system.eventStream, tag)

  StatsActors.getStatsReaper ! WatchMe(self)

  override def receive: Receive = activatedWith(emptyPopRecord[String])

  def activatedWith(current: PopRecord[String]): Receive = {

    case PopularityFound(newAliases) =>
      become(activatedWith(updatePopRecord(newAliases,PopRecord(current.catalog,current.topVoted))))
    case PopularityInquiry =>
      // TODO needs fix for item containing spltr (could be simply quoting perhaps)
      val tsv = current.topVoted.iterator.map{ pop => s"${pop.item}$spltrStr${pop.votes}" }
      val tsvString = if(tsv.nonEmpty) tsv.mkString(";") else onEmptyRes
      sender ! Popularity(s"${tag}${resultBreak}${tsvString}")
    case TopsCardinalityInquiry =>
      sender ! PopularityCardinality(s"${tag}${resultBreak}${current.topVoted.size}")
    case NewEpoch => become(activatedWith(emptyPopRecord[String]))
    case Ping => log.info(s"alive with ${current.toString}")
  }
}
