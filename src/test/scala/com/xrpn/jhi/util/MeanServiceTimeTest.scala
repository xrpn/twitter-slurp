package com.xrpn.jhi.util

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}

/**
  * Created by alsq on 11/23/16.
  */
class MeanServiceTimeTest extends FunSuite with BeforeAndAfterEach with BeforeAndAfterAll {

  import MeanServiceTime._

  test("smoke") {
    assert(1 === 1)
    assert(1 !== Option(1))
  }

  test("compute mean service time unavailable") {
    intercept[AssertionError] {
      computeMeanServiceTime(emptyAgingRecord)
    }
    assert(unavailable === computeMeanServiceTime(Nil))
    assert(unavailable === computeMeanServiceTime(List(emptyAgingRecord)))
    intercept[AssertionError] {
      computeMeanServiceTime(List(emptyAgingRecord, emptyAgingRecord))
    }
  }

  test("compute mean service time no arrivals") {
    val epoch = System.currentTimeMillis
    val aging = epoch + msInSecond
    val noArrivalsRecord = AgingRecord(0,aging,epoch)
    assert(insufficientArrivals === computeMeanServiceTime(noArrivalsRecord))
    assert(unavailable === computeMeanServiceTime(List(noArrivalsRecord)))
    intercept[AssertionError] {
      computeMeanServiceTime(List(noArrivalsRecord, noArrivalsRecord))
    }
  }

  test("compute mean service time 0/0") {
    val st = 0
    val epoch = System.currentTimeMillis
    val aging = epoch
    val arrivals = 0
    val ar0_0 = AgingRecord(arrivals,aging,epoch)
    assert(insufficientArrivals === computeMeanServiceTime(ar0_0))
    assert(unavailable === computeMeanServiceTime(List(ar0_0)))
    intercept[AssertionError] {
      assert(unavailable === computeMeanServiceTime(List(ar0_0, ar0_0)))
    }
  }

  test("compute mean service time 0/1") {
    val st = 2
    val epoch = System.currentTimeMillis
    val aging = epoch + st
    val arrivals = 0
    val ar0_2 = AgingRecord(arrivals, aging, epoch)
    assert(insufficientArrivals === computeMeanServiceTime(ar0_2))
  }

  test("compute mean service time one") {
    val st = 8
    val epoch = System.currentTimeMillis
    val aging = epoch + st
    val arrivals = 1
    val stAsD = 8.0
    val ar1_8 = AgingRecord(arrivals,aging,epoch)
    assert(stAsD === computeMeanServiceTime(ar1_8))
    assert(unavailable === computeMeanServiceTime(List(ar1_8)))
    intercept[AssertionError] {
      computeMeanServiceTime(List(emptyAgingRecord, ar1_8))
    }
    intercept[AssertionError] {
      computeMeanServiceTime(List(ar1_8, ar1_8))
    }
    intercept[AssertionError] {
      computeMeanServiceTime(List(ar1_8, emptyAgingRecord))
    }
    intercept[IllegalStateException] {
      computeMeanServiceTime(List(ar1_8,ar1_8,ar1_8))
    }
  }

  test("compute mean service time two") {
    val st = 8
    val epoch = System.currentTimeMillis
    val aging = epoch + st
    val arrivals = 2
    val stAsD = 4.0
    val ar2_8 = AgingRecord(arrivals,aging,epoch)
    assert(stAsD === computeMeanServiceTime(ar2_8))
    assert(unavailable === computeMeanServiceTime(List(ar2_8)))
    intercept[AssertionError] {
      computeMeanServiceTime(List(emptyAgingRecord,ar2_8))
    }
    intercept[AssertionError] {
      computeMeanServiceTime(List(ar2_8, ar2_8))
    }
    intercept[AssertionError] {
      computeMeanServiceTime(List(ar2_8, emptyAgingRecord))
    }
  }

  test ("update mean service time empty") {
    val aging = System.currentTimeMillis
    val aut = updateAgingRecord(aging,emptyAgingRecord)
    assert(1 === aut.arrivals)
    assert(aging === aut.aging)
    assert(aging === aut.epoch)
  }

  test ("update mean service time non-empty") {
    val st = 2
    val epoch = System.currentTimeMillis
    val aging = epoch + st
    val newAging = epoch + st + st
    val arrivals = 1
    val ar1_2 = AgingRecord(arrivals, aging, epoch)
    val aut = updateAgingRecord(newAging,ar1_2)
    assert(2 === aut.arrivals)
    assert(newAging === aut.aging)
    assert(epoch === aut.epoch)
  }

  test ("update periodic service time empty") {
    val aging = System.currentTimeMillis
    val oracle = List(firstArrival(aging,None))
    val aut: List[AgingRecord] = updatePeriodicServiceTime(aging,Nil,msInSecond)
    assert(aut === oracle)
  }

  test ("update periodic service time illegal") {
    intercept[IllegalStateException] {
      val aging = System.currentTimeMillis
      val oracle = List(firstArrival(aging,None))
      val aut: List[AgingRecord] = updatePeriodicServiceTime(aging, List(emptyAgingRecord, emptyAgingRecord, emptyAgingRecord), msInSecond)
    }
  }

  test ("update periodic service one arrival") {
    val epoch = System.currentTimeMillis
    val period = msInSecond
    val arr: List[AgingRecord] = updatePeriodicServiceTime(epoch,Nil,period)
    assert(1 === arr.size)
    assert(1 === arr.head.arrivals)
    assert(epoch === arr.head.aging)
    assert(epoch === arr.head.epoch)
  }

  test ("update periodic service two arrivals, one new in period") {
    val st = 2
    val period = msInSecond
    val epoch = System.currentTimeMillis
    val aging = epoch + st
    val arr: List[AgingRecord] = updatePeriodicServiceTime(epoch,Nil,period)
    val aut: List[AgingRecord] = updatePeriodicServiceTime(aging,arr,period)
    assert(1 === aut.size)
    assert(2 === aut.head.arrivals)
    assert(aging === aut.head.aging)
    assert(epoch === aut.head.epoch)
  }

  test ("update periodic service two, one in period, one new past period") {
    val st = 2
    val period = msInSecond
    val epoch = System.currentTimeMillis
    val arr: List[AgingRecord] = updatePeriodicServiceTime(epoch,Nil,period)
    val newEpoch = epoch + period
    val aging = newEpoch + st
    val aut: List[AgingRecord] = updatePeriodicServiceTime(aging,arr,period)
    assert(2 === aut.size)

    val npAr1 = AgingRecord(1, aging, newEpoch)
    val ar1 = AgingRecord(1, newEpoch, epoch)
    val oracle = List(npAr1,ar1)
    assert(aut === oracle)
  }

  test ("update periodic service two, one in period, one new past past period") {
    val st = 2
    val period = msInSecond
    val epoch = System.currentTimeMillis
    val arr: List[AgingRecord] = updatePeriodicServiceTime(epoch,Nil,period)
    val nnpEpoch = epoch + period + period
    val aging = nnpEpoch + st
    val aut: List[AgingRecord] = updatePeriodicServiceTime(aging,arr,period)
    assert(2 === aut.size)

    val nnpAr1 = AgingRecord(1, aging, aging)
    val oracle = List(nnpAr1,AgingRecord(0,aging,aging-period))
    assert(aut === oracle)
  }

  test ("update periodic service three, two in period, one new") {
    val st = 2
    val period = msInSecond
    val epoch = System.currentTimeMillis
    val aging = epoch + st
    val arr: List[AgingRecord] = updatePeriodicServiceTime(epoch,Nil,period)
    val arr1: List[AgingRecord] = updatePeriodicServiceTime(aging,arr,period)
    assert(1 === arr1.size)
    val npEpoch = epoch + period
    val npAging = npEpoch + st
    val aut: List[AgingRecord] = updatePeriodicServiceTime(npAging,arr1,period)
    assert(2 === aut.size)

    val ar2_2 = AgingRecord(2, npEpoch, epoch)
    val oracle = List(firstArrival(npAging,Option(npEpoch)),ar2_2)
    assert(aut === oracle)
  }

  test ("update periodic service three, two in period, period lapse, one new") {
    val st = 2
    val period = msInSecond
    val epoch = System.currentTimeMillis
    val aging = epoch + st
    val arr: List[AgingRecord] = updatePeriodicServiceTime(epoch,Nil,period)
    val arr1: List[AgingRecord] = updatePeriodicServiceTime(aging,arr,period)
    assert(1 === arr1.size)
    val nppEpoch = epoch + period + period
    val nppAging = nppEpoch + st
    val aut: List[AgingRecord] = updatePeriodicServiceTime(nppAging,arr1,period)
    assert(2 === aut.size)

    val oracle = List(firstArrival(nppAging,None),AgingRecord(0,nppAging,nppAging-period))
    assert(aut === oracle)
  }

  test ("update periodic service four, two in period, two new") {
    val st = 2
    val period = msInSecond
    val epoch = System.currentTimeMillis
    val arr: List[AgingRecord] = updatePeriodicServiceTime(epoch,Nil,period)
    assert(1 === arr.size)
    val aging = epoch + st
    val arr1: List[AgingRecord] = updatePeriodicServiceTime(aging,arr,period)
    assert(1 === arr1.size)
    val npEpoch = epoch + period
    val npAging = npEpoch + st
    val npArr: List[AgingRecord] = updatePeriodicServiceTime(npAging,arr1,period)
    assert(2 === npArr.size)
    val npAging1 = npEpoch + st + st
    val aut: List[AgingRecord] = updatePeriodicServiceTime(npAging1,npArr,period)
    assert(2 === aut.size)

    val npAr2_2 = AgingRecord(2, npAging1, npEpoch)
    val ar2_2 = AgingRecord(2, npEpoch, epoch)
    val oracle = List(npAr2_2,ar2_2)
    assert(aut === oracle)
  }

  test ("update periodic service five, two in period, two new, one past period") {
    val st = 2
    val period = msInSecond
    val epoch = System.currentTimeMillis
    val aging = epoch + st
    val arr: List[AgingRecord] = updatePeriodicServiceTime(epoch,Nil,period)
    val arr1: List[AgingRecord] = updatePeriodicServiceTime(aging,arr,period)
    val npEpoch = epoch + period
    val npAging = npEpoch + st
    val npArr: List[AgingRecord] = updatePeriodicServiceTime(npAging,arr1,period)
    val npAging1 = npEpoch + st + st
    val npArr1: List[AgingRecord] = updatePeriodicServiceTime(npAging1,npArr,period)
    val nnpEpoch = npEpoch + period
    val nnpAging = nnpEpoch + st
    val aut: List[AgingRecord] = updatePeriodicServiceTime(nnpAging,npArr1,period)
    assert(2 === aut.size)

    val npAr2_2 = AgingRecord(2, nnpEpoch, npEpoch)
    val oracle = List(firstArrival(nnpAging,Option(nnpEpoch)),npAr2_2)
    assert(aut === oracle)
  }

  test ("update periodic service five, two in period, two new, one past past period") {
    val st = 2
    val period = msInSecond
    val epoch = System.currentTimeMillis
    val aging = epoch + st
    val arr: List[AgingRecord] = updatePeriodicServiceTime(epoch,Nil,period)
    val arr1: List[AgingRecord] = updatePeriodicServiceTime(aging,arr,period)
    val npEpoch = epoch + period
    val npAging = npEpoch + st
    val npArr: List[AgingRecord] = updatePeriodicServiceTime(npAging,arr1,period)
    val npAging1 = npEpoch + st + st
    val npArr1: List[AgingRecord] = updatePeriodicServiceTime(npAging1,npArr,period)
    val nnpEpoch = npEpoch + period + period
    val nnpAging = nnpEpoch + st
    val aut: List[AgingRecord] = updatePeriodicServiceTime(nnpAging,npArr1,period)
    assert(2 === aut.size)

    val oracle = List(firstArrival(nnpAging,Option(nnpAging)),AgingRecord(0,nnpAging,nnpAging-period))
    assert(aut === oracle)
  }
}