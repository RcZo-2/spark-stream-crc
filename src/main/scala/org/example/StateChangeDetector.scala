package org.example

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode, Trigger}
import org.example.UserStateHandler.updateState
import org.example.config.GeneralConfig
import org.example.schema.SparkKafkaLoginMsg


object StateChangeDetector {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("StateChangeDetector")

    val spark = SparkSession.builder.config(sparkConf)
      .getOrCreate()

    import spark.implicits._


    val kafkaStream = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", s"${GeneralConfig.kafkaSrvs}")
      .option("subscribe", s"${GeneralConfig.kafkaLoginTopic}")
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "true")
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp")

    val msgSchema = SparkKafkaLoginMsg.getSchema()

    val upStream = kafkaStream.select(
      col("key"),
      from_json(col("value"), msgSchema).as("msgData"),
      col("timestamp")
    ).select(
      col("key").alias("userId"),
      col("msgData.*"),
      //col("timestamp").as("kafkaTs")
    )


    val dqcStream = upStream.select("userId", "loginTime", "locationEng", "deviceId").filter($"userId".isNotNull)
    // TODO amazon Deequ

    val processedStream = dqcStream
      .withWatermark("loginTime", s"${GeneralConfig.appStreamWmk}")
      .groupByKey(row => row.getAs[String]("userId"))
      .flatMapGroupsWithState(OutputMode.Update()
        , GroupStateTimeout.EventTimeTimeout())(updateState)

    processedStream.selectExpr("userId AS key",
        s"to_json(struct(loginTime,prev_locationEng,locationEng,prev_deviceId,deviceId)) AS value")
      .writeStream
      .trigger(Trigger.ProcessingTime(s"${GeneralConfig.appStreamTrgPT}"))
      .format("kafka")
      .outputMode("update")
      .option("kafka.bootstrap.servers", s"${GeneralConfig.kafkaSrvs}")
      .option("topic", s"${GeneralConfig.kafkaNotifyTopic}")
      .option("checkpointLocation", s"${GeneralConfig.appCkptLoc}")
      //.option("asyncProgressTrackingEnabled", "true")
      .start()
      .awaitTermination()

    spark.streams.awaitAnyTermination()
    spark.stop()
  }
}
