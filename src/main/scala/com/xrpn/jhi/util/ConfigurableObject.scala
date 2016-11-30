package com.xrpn.jhi.util

import com.typesafe.config.{Config, ConfigFactory}

/**
  * Marker for what needs access to the Config object.
  * Created by alsq on 11/25/16.
  */
trait ConfigurableObject {
  protected lazy val akkaConfig = ConfigFactory.load()
  def getAkkaConfig: Config = akkaConfig
}
