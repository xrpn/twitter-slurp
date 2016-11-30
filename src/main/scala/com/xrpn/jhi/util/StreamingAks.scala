package com.xrpn.jhi.util

import akka.NotUsed
import akka.stream.scaladsl.{Balance, Flow, GraphDSL, Merge, Source, ZipWith}
import akka.stream.{FlowShape, SourceShape}

import scala.collection.immutable.Stream

/**
  * Created by alsq on 11/25/16.
  */
object StreamingAks {

  /**
    * Map an incoming flow to worker streams, and merge/reduce the results. (Lifted from the docs.)
    */
  def balancer[In, Out](worker: Flow[In, Out, Any], workerCount: Int): Flow[In, Out, NotUsed] = {
    import GraphDSL.Implicits._
    Flow.fromGraph(GraphDSL.create() { implicit b =>
      val balancer = b.add(Balance[In](workerCount, waitForAllDownstreams = true))
      val merge = b.add(Merge[Out](workerCount))
      for (_ <- 1 to workerCount) {
        // for each worker, add an edge from the balancer to the worker, then wire it to the merge element
        balancer ~> worker ~> merge
      }
      FlowShape(balancer.in, merge.out)
    })
  }

  /**
    * Zip a streaming source with sequence numbers for counting.
    */
  def counted[COUNTABLE](toBeCounted: Source[COUNTABLE, NotUsed]): Source[(COUNTABLE, Int), NotUsed] = {
    import akka.stream.scaladsl.GraphDSL.Implicits._
    Source.fromGraph(
      GraphDSL.create() { implicit builder =>
        val zip = builder.add(ZipWith((item:COUNTABLE, seqNum:Int) => (item,seqNum)))
        toBeCounted ~> zip.in0
        Source(Stream from 1) ~> zip.in1
        SourceShape(zip.out)
      }
    )}


}
