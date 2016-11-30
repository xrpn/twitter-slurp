package com.xrpn.jhi.util

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}

/**
  * Created by alsq on 11/23/16.
  */
class MostPopularTest extends FunSuite with BeforeAndAfterEach with BeforeAndAfterAll {

  import MostPopular._

  val epoch = System.currentTimeMillis
  val delta = 100l
  val aux100 = Popularity[Int](0,100,epoch)
  val aux101 = Popularity[Int](2,101,epoch)
  val aux102 = Popularity[Int](4,102,epoch)
  val aux103 = Popularity[Int](7,103,epoch)
  val aux104 = Popularity[Int](9,104,epoch)
  val aux105 = Popularity[Int](5,105,epoch)
  val aux106 = Popularity[Int](3,106,epoch)
  val empty = emptyPopRecord[Int]
  val allPops = List(aux106,aux105,aux104,aux103,aux102,aux101,aux100)
  assert(maxCatalogSize < allPops.size)
  val somePops = List(aux104,aux103,aux102,aux101,aux100)
  assert(maxCatalogSize >= somePops.size)
  val oldItems = allPops.map(pop => pop.item)
  val fullCat = allPops.foldLeft(empty.catalog)((cat, pop) => cat + (pop.item -> pop))
  val allHits = allPops.foldLeft(empty.topVoted)((h, pop) => h+pop)
  assert(allHits.size === allPops.size)
  val someCat = somePops.foldLeft(empty.catalog)((cat, pop) => cat + (pop.item -> pop))
  val someHits = somePops.foldLeft(empty.topVoted)((h, pop) => h+pop)
  assert(someHits.size === somePops.size)


  test("smoke") {
    assert(1 === 1)
    assert(1 !== Option(1))
  }

  test("sortedSet ordering: basic") {
    val empty = emptyPopRecord[Int]
    val hits = empty.topVoted
    val aux0_100 = Popularity[Int](0,100,epoch)
    val aux2_101 = Popularity[Int](2,101,epoch)
    val hits1 = hits + aux0_100
    val hits2 = hits1 + aux2_101
    assert(hits2.iterator.next === aux0_100)
  }

  // this is one of the reasons why Popularity does not work
  ignore("sortedSet ordering") {
    val empty = emptyPopRecord[Int]
    val hits = empty.topVoted
    val aux0_100 = Popularity[Int](0,100,epoch)
    val aux2_101 = Popularity[Int](2,101,epoch)
    val aux2_102 = Popularity[Int](2,102,epoch)
    val hits1 = hits + aux0_100
    assert(1 === hits1.size)
    val hits2 = hits1 + aux2_101
    assert(2 === hits2.size)
    val hits3 = hits2 + aux2_102
    assert(2 === hits3.size) // fails
    assert(hits3.iterator.next === aux0_100)
    val hits4 = hits3 - aux0_100
    assert(1 === hits4.size) // fails
    assert(hits4.iterator.next === aux2_102) // fails
  }

  test("sortedSet updates: confirm my understanding of how this works") {
    val empty = emptyPopRecord[Int]
    val hits = empty.topVoted
    val aux100 = Popularity[Int](0,100,epoch)
    val aux101 = Popularity[Int](2,101,epoch)
    val aux102 = Popularity[Int](4,102,epoch)
    val hits1 = hits + aux100
    val hits2 = hits1 + aux101
    val hits3 = hits2 + aux102
    assert(hits3.iterator.next === aux100)
    val hits4 = hits3 - aux100
    assert(hits4.iterator.next === aux101)
  }

  test("trimcatalog") {
    val empty = emptyPopRecord[Int]
    val hits = empty.topVoted
    val catalog = empty.catalog
    val pops = List(aux106,aux105,aux104,aux103,aux102,aux101,aux100)
    assert(maxCatalogSize < pops.size)
    val allHits = pops.foldLeft(hits)((h, pop) => h+pop)
    val fullCat = pops.foldLeft(catalog)((cat, pop) => cat + (pop.item -> pop))
    val trimmedCapacities = /* -1 will trigger assertion */ List(0,3,5,pops.size,pops.size+2)
    trimmedCapacities.foreach(trimmedCap => {
      val pr = trimLeastVoted(PopRecord(fullCat,allHits))(trimmedCap)
      if (0 > trimmedCap) {
        assert(pr.catalog.isEmpty, "" + trimmedCap)
        assert(pr.topVoted.isEmpty, "" + trimmedCap)
      } else if (pops.size > trimmedCap) {
        assert(trimmedCap === pr.catalog.size, ""+trimmedCap)
        assert(trimmedCap === pr.topVoted.size, ""+trimmedCap)
      } else {
        assert(pops.size === pr.catalog.size, ""+trimmedCap)
        assert(pops.size === pr.topVoted.size, ""+trimmedCap)
      }
      if (0 < trimmedCap && pops.size >= trimmedCap) {
        assert(pr.topVoted.iterator.next === pops(trimmedCap-1),""+trimmedCap)
        pr.topVoted.iterator.foreach(pop => {
          assert(Int.MinValue !== pr.catalog.getOrElse(pop.item, Int.MinValue),""+trimmedCap)
        })
      }
    })
  }

  test("updateBuilder old items") {
    val allHitsAcc = (List[Int](),List[Popularity[Int]](),allHits)
    var iterCount = 0
    oldItems.foldLeft(allHitsAcc)((accum,item) => {
      val newAcc = updateBuilder(fullCat)(accum,item)
      val (items,newPops,newHits) = newAcc
      assert((iterCount+1) === items.size,""+iterCount)
      assert(items.contains(allPops(iterCount).item),""+iterCount)
      assert((iterCount+1) === newPops.size,""+iterCount)
      assert(allPops.size === newHits.size,""+iterCount)
      newPops.foreach(newPop => {
        assert(!allPops.contains(newPop),""+iterCount)
        val samePopsByItem = allPops.filter(pop => pop.item == newPop.item)
        assert(1 === samePopsByItem.size)
        assert(samePopsByItem.head.votes+1 === newPop.votes)
        assert(!allHits.equals(newHits),""+iterCount)
        assert(!allHits.contains(newPop),""+iterCount)
        assert(newHits.contains(newPop),""+iterCount)
      })
      assert(!newHits.contains(allPops(iterCount)),""+iterCount)
      iterCount = iterCount + 1
      newAcc
    })
  }

  test("updateBuilder new items") {
    val noHitsAcc = (List[Int](),List[Popularity[Int]](),empty.topVoted)
    var iterCount = 0
    oldItems.foldLeft(noHitsAcc)((accum,item) => {
      val newAcc = updateBuilder(empty.catalog)(accum,item)
      val (items,newPops,newHits) = newAcc
      assert((iterCount+1) === items.size,""+iterCount)
      assert(items.contains(allPops(iterCount).item),""+iterCount)
      assert((iterCount+1) === newPops.size,""+iterCount)
      assert(newPops.size === newHits.size,""+iterCount)
      newPops.foreach(newPop => {
        assert(!allPops.contains(newPop),""+iterCount)
        assert(1 === newPop.votes)
        assert(!allHits.contains(newPop),""+iterCount)
        assert(newHits.contains(newPop),""+iterCount)
      })
      assert(!newHits.contains(allPops(iterCount)),""+iterCount)
      iterCount = iterCount + 1
      newAcc
    })
  }

  test("updateBuilder mix old and new partitioned") {
    val additionalPops = List(aux106,aux105)
    val mixedPops = somePops ::: additionalPops
    assert(allPops.toSet.equals(mixedPops.toSet))
    val mixedItems = mixedPops.map(pop => pop.item)

    val someHitsAcc = (List[Int](),List[Popularity[Int]](),someHits)
    var iterCount = 0
    val res = mixedItems.foldLeft(someHitsAcc)((accum,item) => {
      val newAcc = updateBuilder(someCat)(accum,item)
      val (items,newPops,newHits) = newAcc
      assert((iterCount+1) === items.size,""+iterCount)
      assert(items.contains(mixedPops(iterCount).item),""+iterCount)
      assert((iterCount+1) === newPops.size,""+iterCount)
      newPops.foreach(newPop => {
        assert(!allPops.contains(newPop),""+iterCount)
        val samePopsByItem = somePops.filter(pop => pop.item == newPop.item)
        if (1 === samePopsByItem.size) assert(samePopsByItem.head.votes+1 === newPop.votes)
        else assert(1 === newPop.votes)
        assert(!allHits.contains(newPop),""+iterCount)
        assert(newHits.contains(newPop),""+iterCount)
      })
      assert(!newHits.contains(allPops(iterCount)),""+iterCount)
      iterCount = iterCount + 1
      newAcc
    })
    val (items,pops,hits) = res
    assert(hits.size === mixedItems.size)
    assert(pops.size === mixedItems.size)
    assert(items.size === mixedItems.size)
  }

  test("updateBuilder mix old and new interleaved") {
    val mixedPops = List(aux104,aux103,aux106,aux102,aux101,aux105,aux100)
    assert(allPops.toSet.equals(mixedPops.toSet))
    val mixedItems = mixedPops.map(pop => pop.item)

    val someHitsAcc = (List[Int](),List[Popularity[Int]](),someHits)
    var iterCount = 0
    val res = mixedItems.foldLeft(someHitsAcc)((accum,item) => {
      val newAcc = updateBuilder(someCat)(accum,item)
      val (items,newPops,newHits) = newAcc
      assert((iterCount+1) === items.size,""+iterCount)
      assert(items.contains(mixedPops(iterCount).item),""+iterCount)
      assert((iterCount+1) === newPops.size,""+iterCount)
      newPops.foreach(newPop => {
        assert(!allPops.contains(newPop),""+iterCount)
        val samePopsByItem = somePops.filter(pop => pop.item == newPop.item)
        if (1 === samePopsByItem.size) assert(samePopsByItem.head.votes+1 === newPop.votes)
        else assert(1 === newPop.votes)
        assert(!allHits.contains(newPop),""+iterCount)
        assert(newHits.contains(newPop),""+iterCount)
      })
      assert(!newHits.contains(allPops(iterCount)),""+iterCount)
      iterCount = iterCount + 1
      newAcc
    })
    val (items,pops,hits) = res
    assert(hits.size === mixedItems.size)
    assert(pops.size === mixedItems.size)
    assert(items.size === mixedItems.size)
  }

  test("updatePopRecord without trim") {
    val additionalPops = List(aux106,aux105)
    val additionalItems = additionalPops.map(pop => pop.item)
    val newPopr = PopRecord(someCat,someHits)
    val updated = updatePopRecord(additionalItems,newPopr)(fullCat.size)
    assert(updated.catalog.keySet === fullCat.keySet)
    assert(updated.topVoted.size === allHits.size)
    // only the items match, the number of votes is different
    assert(updated.topVoted.map(pop => pop.item) === allHits.keySet.map(pop => pop.item))
    updated.topVoted.foreach( hit => assert(updated.catalog(hit.item) === hit))
    updated.topVoted.exists( hit => !fullCat(hit.item).equals(hit))
  }

  test("updatePopRecord with trim") {
    val additionalPops = List(aux106,aux105)
    val additionalItems = additionalPops.map(pop => pop.item)
    val newPopr = PopRecord(someCat,someHits)
    val updated = updatePopRecord(additionalItems,newPopr)(maxCatalogSize)
    assert(updated.catalog.size === maxCatalogSize)
    assert(updated.topVoted.size === maxCatalogSize)
    updated.topVoted.foreach( hit => assert(updated.catalog(hit.item) === hit))
  }
}
