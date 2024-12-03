package org.example

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode, Trigger}
import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}
import org.example.UserStateHandler.updateState


object StateChangeDetector {
  private final val arg_kafka_servers: String = sys.env.getOrElse("ARG_KAFKA_SERVERS", "default_value")

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("StateChangeDetector")

    lazy val spark = SparkSession.builder.config(sparkConf)
      .getOrCreate()

    import spark.implicits._

    val msgSchema = StructType(Seq(
      StructField("serviceItem", StringType),
      StructField("functionItem", StringType),
      StructField("loginTime", TimestampType),
      StructField("loginType", StringType),
      StructField("clientIp", StringType),
      StructField("locationEng", StringType),
      StructField("deviceId", StringType),
      StructField("otpStatus", StringType),
      StructField("dataSource", StringType)
    ))

    val kafkaStream = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", s"${arg_kafka_servers}")
      .option("subscribe", "test1")
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "true")
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp")


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
      .withWatermark("loginTime", "3 minute")
      .groupByKey(row => row.getAs[String]("userId"))
      .flatMapGroupsWithState(OutputMode.Update()
        , GroupStateTimeout.EventTimeTimeout())(updateState)

    processedStream.selectExpr("userId AS key",
        s"to_json(struct(loginTime,prev_locationEng,locationEng,prev_deviceId,deviceId)) AS value")
      .writeStream
      .trigger(Trigger.ProcessingTime("1 seconds"))
      .format("kafka")
      .outputMode("update")
      .option("kafka.bootstrap.servers", s"${arg_kafka_servers}")
      .option("topic", "test2")
      .option("checkpointLocation", "./ckpt_state")
      //.option("asyncProgressTrackingEnabled", "true")
      .start()
      .awaitTermination()

    spark.streams.awaitAnyTermination()
    spark.stop()
  }
}
