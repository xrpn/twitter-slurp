package com.xrpn.jhi.util
import com.typesafe.scalalogging.LazyLogging


/**
  * Utility functionality helping keep a running tally of mean service time.
  * Created by alsq on 11/22/16.
  */
object MeanServiceTime {

  val msInSecond = 1000l
  val msInMinute = msInSecond*60l
  val msInHour = msInMinute*60l
  val emptyEpoch = Long.MinValue
  // marker for cases where not enough arrivals are available
  val insufficientArrivals = -2.0
  // marker for cases where results cannot be computed for sundry reasons
  val unavailable = -1.0
  val emptyAgingRecord = AgingRecord(0,emptyEpoch,emptyEpoch)

  def mstAsString(res: Double): String = {
    res match {
      case `unavailable` => "(unavailable)"
      case `insufficientArrivals` => "(premature)"
      case st@_ => f"$st%.2f"
    }
  }

  /**
    * Keeps track of the state necessary to compute mean arrival (or service) time.
    * The latter is the mean duration between arrivals for the time interval beginning
    * at epoch and ending at aging.
    * @param arrivals how many arrivals since epoch
    * @param aging timestamp of the last arrival
    * @param epoch timestamp of the beginning of time, for this period.
    */
  case class AgingRecord(arrivals: Int, aging: Long, epoch: Long) extends LazyLogging {
    val sanity = (epoch > 0 && (epoch <= aging || aging == emptyEpoch) && 0 <= arrivals) ||
      (0 == arrivals && epoch == emptyEpoch && aging == emptyEpoch)
    assert(sanity)
    if (!sanity) {
      val here = (new Throwable()).getStackTrace().mkString("\t", "\n", "\n")
      logger.warn(s"expect insensible behavior: arrivals: $arrivals, aging: $aging, epoch: $epoch at\n$here")
    }
  }

  /**
    * Given an AgingRecord, compute the mean arrival/service time.
    * @param ar the input data to the computation
    * @return the mean interval between events
    */
  private def cMST(ar: AgingRecord): Double = {
    assert(!emptyAgingRecord.equals(ar))
    if (1 > ar.arrivals) insufficientArrivals
    else if (1 == ar.arrivals && 0 == ar.aging - ar.epoch)
      // TODO figure out how to remove this corner case
      insufficientArrivals
    else {
      val timeLapse: Double = ar.aging - ar.epoch
      val arr: Double = ar.arrivals
      timeLapse / arr
    }
  }

  /**
    * Convenience function for readability.
    * @param aging the time of the first arrival
    * @return the AgingRecord for this first arrival
    */
  private[util] def firstArrival(aging: Long, prevEpoch: Option[Long]): AgingRecord = prevEpoch match {
    case None => updateAgingRecord(aging,emptyAgingRecord)
    case Some(epoch) => updateAgingRecord(aging,AgingRecord(0,emptyEpoch,epoch))
  }

  // java-style function overloading in scala tends to mess things up; the
  // idiomatic way to "overload" is to use a magnet patterns, as done here

  sealed trait CmstMagnet {
    type Result
    def apply(): Result
  }

  // "overload" computeMeanServiceTime for AgingRecord or List[AgingRecord]

  object CmstMagnet {
    implicit def fromAgingRecord(ar: AgingRecord) = new CmstMagnet {
      type Result = Double
      def apply(): Result = cMST(ar)
    }

    implicit def fromAgingRecordList(ar: List[AgingRecord]) = new CmstMagnet {
      type Result = Double
      def apply(): Result = ar match {
        case Nil =>
          unavailable
        case st :: Nil =>
          unavailable
        case st1 :: st2 :: Nil =>
          assert(st1 != emptyAgingRecord)
          assert(st1 != st2)
          cMST(st2)
        case st1 :: rest => throw new IllegalStateException()
      }
    }
  }

  // overloading implemented via magnet pattern

  def computeMeanServiceTime(magnet: CmstMagnet): magnet.Result = magnet()

  /**
    * Created a new AgingRecord updated with the new arrival at time "aging"
    * @param aging time of new arrival
    * @param ar the previous AgingRecord
    * @return an updated AgingRecord, including the latest arrival
    */
  def updateAgingRecord(aging: Long, ar: AgingRecord): AgingRecord = {
    assert(0 < aging)
    if (emptyEpoch == ar.epoch) AgingRecord(ar.arrivals + 1, aging, aging)
    else AgingRecord(ar.arrivals + 1, aging, ar.epoch)
  }

  /**
    * Arrival times for a limited period (vs. since the beginning of time) consist
    * of two AgingRecord items, one (completed) for the period just expired, and one
    * (actively updated) for the current period.  Since we do not have yet all the
    * information for the current period (because it has not yet expired), the
    * completed AgingRecord for the period just expired provides the current stats,
    * while the current aging record is updated.  So the stats are somewhat stale.
    * <br><br>
    * This scheme is simpler than keeping a tally of running stats; for short periods
    * it works well.  For longer periods it is approximate--but it is still simple ;)
    *
    * @param aging
    * @param ar
    * @param period
    * @return the updated state
    */
  def updatePeriodicServiceTime(aging: Long, ar: List[AgingRecord], period: Long): List[AgingRecord] = {
    assert(0 < aging && 0 < period)
    ar match {
      case Nil => List(firstArrival(aging,None)) // sets epoch to aging
      case active :: Nil =>
        (aging - active.epoch) / period match {
          case 0 =>/* arrival during this period */ List(updateAgingRecord(aging,active))
          case 1 => /* next period */ List(firstArrival(aging,Option(active.epoch+period)),AgingRecord(active.arrivals,active.epoch+period,active.epoch))
          case more => /* lapsed without arrivals */ List(firstArrival(aging,None),AgingRecord(0,aging,aging-period))
        }
      case active :: last :: Nil =>
        (aging - active.epoch) / period match {
          case 0 => /* update active */ List(updateAgingRecord(aging,active),last)
          case 1 => /* replace last */ List(firstArrival(aging,Option(active.epoch+period)),AgingRecord(active.arrivals,active.epoch+period,active.epoch))
          case more => /* lapsed */ List(firstArrival(aging,None),AgingRecord(0,aging,aging-period))
        }
      case st :: rest => throw new IllegalStateException(ar.mkString("|"))
    }
  }

  /**
    * Shorthand for updatePeriodicServiceTime with a period of one hour.
    */
  def updateHourlyServiceTime(aging: Long, har: List[AgingRecord]) = updatePeriodicServiceTime(aging, har, msInHour)

  /**
    * Shorthand for updatePeriodicServiceTime with a period of one minute.
    */
  def updateMinutelyServiceTime(aging: Long, mar: List[AgingRecord]) = updatePeriodicServiceTime(aging, mar, msInMinute)

  /**
    * Shorthand for updatePeriodicServiceTime with a period of one second.
    */
  def updateSecondlyServiceTime(aging: Long, sar: List[AgingRecord]) = updatePeriodicServiceTime(aging, sar, msInSecond)

}
