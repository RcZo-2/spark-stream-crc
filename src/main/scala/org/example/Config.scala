package org.example

import com.typesafe.config.{Config, ConfigFactory}

object Config {
  val config: Config = ConfigFactory.load()

  val kafkaSrvs: String = config.getString("kafka.servers")
  val mongoUri: String = config.getString("mongo.uri")
  val mongoDB: String = config.getString("mongo.db")
  val mongoColl: String = config.getString("mongo.collection")
}
