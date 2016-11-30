package com.xrpn.jhi.util

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}

/**
  * Created by alsq on 11/23/16.
  */
class PercentOfTotalTest extends FunSuite with BeforeAndAfterEach with BeforeAndAfterAll {

  import PercentOfTotal._

  test("smoke") {
    assert(1 === 1)
    assert(1 !== Option(1))
  }

  test("zero") {
    val pcr = PercentRecord(0,27)
    assert(0.0 === computePercent(pcr))
  }

  test("happy") {
    val pcr = PercentRecord(2,100)
    assert(2.0 === computePercent(pcr))
  }

  test("bad 1") {
    val pcr = PercentRecord(0,0)
    assert(unavailable === computePercent(pcr))
  }

  test("bad 2") {
    val pcr = PercentRecord(1,0)
    assert(unavailable === computePercent(pcr))
  }
}
