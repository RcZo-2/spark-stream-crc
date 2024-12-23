package org.example.config

import com.typesafe.config.{Config, ConfigFactory}
import org.example.config.ConfigLoader.loadConfig

private object ConfigLoader {
  def loadConfig(): Config = {
    val baseConfig = ConfigFactory.load()
    val env = baseConfig.getString("environment.env")
    val envConfig = ConfigFactory.parseResources(s"config/application-$env.conf")
    baseConfig.withFallback(envConfig)
  }
}

object GeneralConfig {

  val config = loadConfig()

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
