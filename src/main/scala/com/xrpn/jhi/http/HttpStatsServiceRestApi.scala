package com.xrpn.jhi.http

import akka.actor._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import akka.pattern.ask
import akka.util.Timeout
//import com.xrpn.jhi.aka.StatsActors

import scala.concurrent.ExecutionContext

/**
  * Ready-to-bind wrapper.
  * @param system the system for the service actor
  * @param timeout http request timeout
  */
class HttpStatsServiceRestApi(val system: ActorSystem, val timeout: Timeout) extends HttpStatsServiceRoutes {
  implicit val requestTimeout = timeout
  implicit def executionContext = system.dispatcher

  override def statsServiceCreator = system.actorOf(HttpStatsServiceAka.props1(timeout), HttpStatsServiceAka.defaultActorName)

}

/**
  * Named HTTP access points for the REST api.
  */
trait HttpStatsServiceRoutes extends HttpStatsServiceApi {

  import StatusCodes._
  implicit val system:ActorSystem

  def scRoutes: Route = helpRoute ~ meantimeRoute ~ tweetcountRoute ~ percentRoute ~ popsEmojiRoute ~ popsHashtagRoute ~ popsTldRoute

  def helpRoute =
    pathPrefix("help") {
      pathEndOrSingleSlash {
        get {
          // GET /help
          onSuccess(getPing) { pingMsg =>
            complete(OK, pingMsg)
          }
        }
      }
    }
  def meantimeRoute =
    pathPrefix("meantime") {
      pathEndOrSingleSlash {
        get {
          // GET /meantime
          onSuccess(getMeanTime) { allStats =>
            complete(OK, allStats)
          }
        }
      }
    }
  def tweetcountRoute =
    pathPrefix("tweetcount") {
      pathEndOrSingleSlash {
        get {
          // GET /tweetcount
          onSuccess(getTweetCount) { allStats =>
            complete(OK, allStats)
          }
        }
      }
    }
  def percentRoute =
    pathPrefix("percent") {
      pathEndOrSingleSlash {
        get {
          // GET /percent
          onSuccess(getPercent) { allStats =>
            complete(OK, allStats)
          }
        }
      }
    }
  def popsEmojiRoute =
    pathPrefix("topemoji") {
      pathEndOrSingleSlash {
        get {
          // GET /topemoji
          onSuccess(getTopEmoji) { allStats =>
            complete(OK, allStats)
          }
        }
      }
    }
  def popsHashtagRoute =
    pathPrefix("tophashtag") {
      pathEndOrSingleSlash {
        get {
          // GET /tophashtag
          onSuccess(getTopHashtag) { allStats =>
            complete(OK, allStats)
          }
        }
      }
    }
  def popsTldRoute =
    pathPrefix("toptld") {
      pathEndOrSingleSlash {
        get {
          // GET /toptld
          onSuccess(getTopTld) { allStats =>
            complete(OK, allStats)
          }
        }
      }
    }
}

/**
  * Bridge between named HTTP access points and the service actor.
  */
trait HttpStatsServiceApi {
  import HttpStatsServiceAka._

  implicit def executionContext: ExecutionContext
  implicit def requestTimeout: Timeout

  def statsServiceCreator: ActorRef
  lazy val statsService: ActorRef = statsServiceCreator
  def statsServiceAkaName = statsService.toString

  def getPing = statsService.ask(Ping).mapTo[String]
  def getMeanTime = statsService.ask(MeanTime).mapTo[String]
  def getPercent = statsService.ask(Percent).mapTo[String]
  def getTweetCount = statsService.ask(TweetCount).mapTo[String]
  def getTopEmoji = statsService.ask(TopEmoji).mapTo[String]
  def getTopHashtag = statsService.ask(TopHashtag).mapTo[String]
  def getTopTld = statsService.ask(TopTld).mapTo[String]
}
