package com.xrpn.jhi.aks

import com.xrpn.jhi.t4j.T4JStreamer.rawBuffer
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}

/**
  * Created by alsq on 11/19/16.
  */
class IterableJQTest extends FunSuite with BeforeAndAfterEach with BeforeAndAfterAll {


  override def beforeEach(): Unit = {
    rawBuffer.clear()
  }

  test ("smoke") {
    assert (1 === 1)
    assert (1 !== Option(1))
    assert(rawBuffer.isEmpty)
  }

  test ("empty") {
    val aut = IterableJQ(rawBuffer,10)
    assert(aut.iterator.hasNext)
    assert(aut.iterator.next.isEmpty)
  }

  test ("refill, extract, test for empty") {
    val oracle = "dcacdewceasere"
    val aut = IterableJQ(rawBuffer,10)
    rawBuffer.offer(oracle)
    assert(aut.iterator.hasNext)
    val aut1 = aut.iterator.next
    assert(aut1.contains(oracle))
    assert(aut.iterator.hasNext)
    assert(aut.iterator.next.isEmpty)
  }


}
