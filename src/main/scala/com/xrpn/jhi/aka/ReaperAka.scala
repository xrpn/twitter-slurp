package com.xrpn.jhi.aka

import akka.actor.{Actor, ActorRef, Props, Terminated}
import akka.event.Logging

import scala.collection.mutable.ArrayBuffer

/**
  * Largely lifted from http://letitcrash.com/post/30165507578/shutdown-patterns-in-akka-2
  * Created by alsq on 11/25/16.
  */
object ReaperAka {
  case class WatchMe(ref: ActorRef)

  val props1 = Props[ReaperAka]
  val defaultActorName = "system-reaper"
}

class ReaperAka extends Actor {
  import ReaperAka._
  val log = Logging(this)

  // Keep track of what we're watching
  val watched = ArrayBuffer.empty[ActorRef]

  // Derivations need to implement this method.  It's the
  // hook that's called when everything's dead
  def allSoulsReaped(): Unit = {
    log.warning(s"shutting down system <${context.system.name}>")
    context.system.terminate()
  }

  // Watch and check for termination
  final def receive = {
    case WatchMe(ref) =>
      context.watch(ref)
      watched += ref
      log.info(s"now watching: ${ref.toString}")
    case Terminated(ref) =>
      watched -= ref
      log.info(s"now dead: ${ref.toString}")
      if (watched.isEmpty) allSoulsReaped()
  }
}