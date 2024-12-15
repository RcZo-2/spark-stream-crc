package org.example

import com.typesafe.config.{Config, ConfigFactory}

object Config {
  val config: Config = ConfigFactory.load()

  val kafkaSrvs: String = config.getString("kafka.servers")
  val kafkaLoginTopic: String = config.getString("kafka.loginTopic")
  val kafkaNotifyTopic: String = config.getString("kafka.notifyTopic")
  val mongoUri: String = config.getString("mongo.uri")
  val mongoDB: String = config.getString("mongo.db")
  val mongoColl: String = config.getString("mongo.collection")
  val appStreamWmk: String = config.getString("app.streamWatermark")
  val appStreamTrgPT: String = config.getString("app.streamTriggerProcessingTime")
  val appCkptLoc: String = config.getString("app.checkpointLocation")
  val appAnomalyTFraS: Int = config.getInt("app.anomalyTimeFrameSeconds")
}
