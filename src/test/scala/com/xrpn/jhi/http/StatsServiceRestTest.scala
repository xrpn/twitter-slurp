package com.xrpn.jhi.http

import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.xrpn.jhi.aka.StatsActors
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}

class StatsServiceRestTest extends FunSuite
  with BeforeAndAfterAll
  with BeforeAndAfterEach
  with ScalatestRouteTest
{

  val scr = new HttpStatsServiceRestApi(system,StatsActors.statsDefaultTimeout)
  val routes = scr.scRoutes

  test("help") {
    Get("/help") ~> routes ~> check {
      assert(handled)
      val ras = responseAs[String]
      assert(HttpStatsServiceAka.pingReply === ras)
    }
  }

}
