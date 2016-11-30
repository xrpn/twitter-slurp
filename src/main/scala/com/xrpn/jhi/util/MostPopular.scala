package com.xrpn.jhi.util

import com.typesafe.scalalogging.LazyLogging

import scala.annotation.tailrec
import scala.collection.immutable.SortedSet

/**
  * Utility functionality to keep track of popular items.
  * Created by alsq on 11/23/16.
  */

object Popularity {

  /**
    * Sort Popularity by votes, ascending (least voted first).  In the library implementation of SortedSet,
    * identity (e.g., the outcome of "contains") is established using the assigned ordering, so provide for
    * multiple distinct Popularity entries with the same votes when aging does not matter.
    * @tparam ITEM arbitrary type for the popular item
    * @tparam POP Popularity or its subclass, the type being sorted
    * @return ordering for POP, least voted first, for as many distinct items as needed, disregarding aging.
    */
  implicit def orderingByVoteAsc[ITEM, POP <: Popularity[ITEM]]: Ordering[POP] = Ordering.by(p => (p.votes, p.item.hashCode))
}

/**
  * Associate an item with its votes or occurrences, and the timestamp
  * of the latest vote or occurrence.  Votes are the metric that determines
  * the (relative) popularity of item.  Higher value of votes means greater
  * popularity.
  *
  * @param item what we are tracking the popularity of
  * @param votes how many times the item is considered popular
  * @param aging timestamp of the last time the votes was changed
  * @tparam ITEM arbitrary type for the popular item
  */
case class Popularity[ITEM](item: ITEM, votes: Int, aging: Long)

object MostPopular extends ConfigurableObject {

  import Popularity._

  /**
    * Configuration key prefix.
    */
  val myKey = "popularity-processor"

  /**
    * The catalog of most popular items.
    */
  type CATALOG[ITEM] = Map[ITEM,Popularity[ITEM]]

  /**
    * The most popular items ordered by popularity, least popular first
    */
  type HITS[ITEM] = SortedSet[Popularity[ITEM]]

  /**
    *
    * @tparam ITEM arbitrary type for the popular item
    * @return
    */
  def emptyPopRecord[ITEM] = PopRecord[ITEM](
    Map[ITEM,Popularity[ITEM]](),
    SortedSet[Popularity[ITEM]]()(orderingByVoteAsc[ITEM,Popularity[ITEM]])
  )

  /**
    * How to retrieve maxCatalogSize from the configuration file.
    */
  val maxCatalogSizeKey = "max-cat-size"

  /**
    * How many items at most will be in the running catalog of topVoted.
    */
  implicit val maxCatalogSize = akkaConfig.getInt(s"$myKey.$maxCatalogSizeKey")

  /**
    * The state necessary to maintain a popularity tally easy to update
    * @param catalog the comprehensive catalog of already popular items
    * @param topVoted popularity sorted by vote, lowest votes first
    * @tparam ITEM arbitrary type for the popular item
    */
  case class PopRecord[ITEM](catalog: CATALOG[ITEM], topVoted: HITS[ITEM]) extends LazyLogging {
    val sanity = catalog.size == topVoted.size
    assert(sanity)
    if (!sanity) {
      val here = (new Throwable()).getStackTrace().mkString("\t", "\n", "\n")
      logger.warn(s"expect insensible behavior: catalog size: ${catalog.size}, top voted size: ${topVoted.size} at\n$here")
    }
  }

  /**
    * INTERNAL ONLY.  Given the current catalog, and the item voted on, (a) accumulate the state needed
    * to update the catalog with the new arrival, and (b) allow for updates in the ranking of voted
    * items to account for the new vote.  (This is a function primarily intended for use in a foldLeft.)
    * @param ctl the current catalog
    * @param transient accumulator for (items,pops); also allows for changes in topVoted
    * @param incomingItem the item that has just been voted on.  It may or may not already
    *                     be in the current catalog.
    * @tparam ITEM arbitrary type for the popular item
    * @return accumulator now comprising this vote
    */
  private[util] def updateBuilder[ITEM](ctl: CATALOG[ITEM])
    (transient:(List[ITEM],List[Popularity[ITEM]],HITS[ITEM]), incomingItem: ITEM) =
  {
    val (items, pops, hits) = transient
    assert(ctl.size <= hits.size)
    val aging = System.currentTimeMillis
    if (!ctl.keySet.contains(incomingItem)) {
      val newPop = Popularity(incomingItem, 1, aging)
      val newPops = newPop :: pops
      val updatedHits = hits + newPop
      (incomingItem :: items, newPops, updatedHits)
    } else {
      val pop = ctl(incomingItem) // guaranteed to be there :)
      val newPop = Popularity(incomingItem, pop.votes + 1, aging)
      val updatedHits = hits - pop + newPop
      val newPops = newPop :: pops
      (incomingItem :: items, newPops, updatedHits)
    }
  }

  /**
    * INTERNAL ONLY.  Apply a fixed aging policy.  Given a PopRecord with aging state of indefinite size,
    * apply the aging policy defined as "remove least voted items until at most maxKeep items remain."  This
    * policy is suboptimal, and the implementation (even if correct) provides questionable results.  However,
    * it is exceptionally simpler than a full-blown aging policy, which would require more state and more
    * complex processing.
    * @param current the PopRecord to be inspected
    * @param maxKeep the max number of items to be retained according to the fixed aging policy.
    * @tparam ITEM arbitrary type for the popular item
    * @return a possibly new PopRecord that conforms with the aging policy
    */
  @tailrec
  private[util] final def trimLeastVoted[ITEM](current: PopRecord[ITEM])(implicit maxKeep: Int): PopRecord[ITEM] = {
    if (maxKeep >= current.catalog.size) {
      current
    } else {
      val hitsIter = current.topVoted.iterator
      assume(hitsIter.hasNext) /* triggered also if maxKeep is less than 0, which should never happen */
      val leastVoted = hitsIter.next
      val sizedCatalog = current.catalog - leastVoted.item
      val sizedTopVoted = current.topVoted - leastVoted
      trimLeastVoted(PopRecord(sizedCatalog, sizedTopVoted))(maxKeep)
    }
  }

  /**
    * Given a PopRecord, build a new PopRecord updated for the newly voted items in newItems.
    * @param newItems the newest items that have beem voted on
    * @param current the PopRecord that is current prior to the arrival of the newest items
    * @param maxKeep maximum allowed size of the hit-parade collection
    * @tparam ITEM arbitrary type for the popular item
    * @return a newly build PopRecord that is inclusive of the newest items
    */
  def updatePopRecord[ITEM](newItems: List[ITEM], current: PopRecord[ITEM])(implicit maxKeep: Int): PopRecord[ITEM] = {
    val maxSize = if (maxKeep >= 0) maxKeep else 0
    val updates: (List[ITEM], List[Popularity[ITEM]], HITS[ITEM]) =
      newItems.foldLeft((List[ITEM](),List[Popularity[ITEM]](),current.topVoted))(updateBuilder(current.catalog))
    val (updatedItems,newPops,newHits) = updates /* newHits now comprises newItems, but catalog is not updated yet */
    assert(updatedItems.size == newPops.size)
    val catalogDelta = updatedItems zip newPops
    val newCatalog: Map[ITEM, Popularity[ITEM]] = current.catalog ++ catalogDelta /* newCatalog now comprises newItems */
    trimLeastVoted(PopRecord(newCatalog,newHits))(maxSize)
  }
}
