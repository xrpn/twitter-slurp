package com.xrpn.jhi.util

import com.typesafe.scalalogging.LazyLogging

/**
  * Utility functionality to keep track of percent values.
  * Created by alsq on 11/23/16.
  */
object PercentOfTotal {

  val unavailable: Double = -1.0
  val emptyPercentRecord = PercentRecord(0,0)

  /**
    * Keep track of the state necessary to compute a percent
    * @param count the running accumulator of viable candidates
    * @param total the running total of all candidates
    */
  case class PercentRecord(count: Int, total: Int) extends LazyLogging {
    val sanity = count >= 0 && total >= 0
    assert(sanity)
    if (!sanity) {
      val here = (new Throwable()).getStackTrace().mkString("\t", "\n", "\n")
      logger.warn(s"expect insensible behavior: currCount: $count, total: $total at\n$here")
    }
  }

  /**
    * Compute the percent
    * @param pr current state
    * @return percent value corresponding to current state
    */
  def computePercent(pr: PercentRecord) = {
    if (1 > pr.total) unavailable
    else {
      val tot: Double = pr.total.toDouble
      val n: Double = pr.count.toDouble
      n*100.0/tot
    }
  }

}
