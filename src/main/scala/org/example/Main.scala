package org.example;

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.example.GroupLoginGuardLogic.runAnomalyUserStateProcess
import org.example.config.ConfigManager
import org.example.schema.{SparkKafkaLoginInput, SubsidiaryOutput}
import org.example.utils.SecretManager


object Main {
  private final val logger = Logger.getLogger(Main.getClass)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("CathayRiskCenterGroupLoginGuard")
      .getOrCreate()

    import spark.implicits._

    logger.info("---------- Switch env config ----------")
    val objectMapper = new ObjectMapper()
    objectMapper.registerModule(DefaultScalaModule)
    val argMap: Map[String, Any] = objectMapper.readValue(args(0), classOf[Map[String, Any]])
    System.setProperty("ENV", argMap("ENV").asInstanceOf[String])
    val config = ConfigManager.getConfig

    logger.info("---------- Get secret ----------")
    val secretMap: Map[String, Any] = objectMapper.readValue(
      SecretManager.getSecret(config.getString("aws.secretName"), config.getString("aws.region")), classOf[Map[String, Any]]
    )
    val kafkaUsername: String = secretMap("username").asInstanceOf[String]
    val kafkaPassword: String = secretMap("password").asInstanceOf[String]


    logger.info("---------- Spark started ----------");

    val kafkaParams = Map[String, String](
      "kafka.bootstrap.servers" -> s"${config.getString("kafka.servers")}",
      "kafka.security.protocol" -> "SASL_SSL",
      "kafka.sasl.mechanism" -> "SCRAM-SHA-512",
      "kafka.sasl.jaas.config" -> s"${config.getString("kafka.scramLoginModule")} required username=\'${kafkaUsername}\' password=\'${kafkaPassword}\';"
    )

    //println(parseTimeExpr(config.getString("app.anomalousMoveTimeThreshold")))

    val kafkaStream = spark
      .readStream
      .format("kafka")
      .options(kafkaParams)
      .option("subscribe", "CDIP-CUB-DAT-R-LOGIN,CDIP-CXL-DAT-R-LOGIN,CDIP-SEC-DAT-R-LOGIN")
      .option("startingOffsets", config.getString("kafka.startingOffsets"))
      .option("failOnDataLoss", config.getString("kafka.failOnDataLoss"))
      .load()
      .selectExpr("CAST(topic AS STRING)", "CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp")

    val msgSchema = SparkKafkaLoginInput.getSchema

    val upStream = kafkaStream.select(
      col("topic"),
      col("key"),
      from_json(col("value"), msgSchema).as("msg_data"),
      col("timestamp")
    ).select(
      col("topic").alias("subsidiary"),
      col("key").alias("customer_id"),
      col("msg_data.*"),
      col("timestamp").as("kafka_ts")
    )

    val dqcStream = upStream
      .select("customer_id",
        "subsidiary",
        "login_date",
        "login_country_code",
        "app_device_id", // substitute UUID
        "web_device_id",
        "model",
        "kafka_ts")
      .filter($"customer_id".isNotNull)
    // TODO amazon Deequ

    val processedStream = dqcStream
      .withWatermark("kafka_ts", config.getString("app.streamWatermark"))
      .groupByKey(row => row.getAs[String]("customer_id"))
      .flatMapGroupsWithState(OutputMode.Update()
        , GroupStateTimeout.ProcessingTimeTimeout())(runAnomalyUserStateProcess)
    //, GroupStateTimeout.EventTimeTimeout())(runAnomalyUserStateProcess)


    //    val query = processedStream
    //      .filter($"subsidiary".equalTo("CDIP-CUB-DAT-R-LOGIN"))
    //      .writeStream
    //      .outputMode("update")
    //      .format("console")
    //      .start()
    //      .awaitTermination()

    //val lkhsOutputFields = classOf[SparkLkhsAnomalyOutput].getDeclaredFields.map(_.getName).mkString(",")
    val subsidiaryOutputFields = classOf[SubsidiaryOutput].getDeclaredFields.map(_.getName).mkString(",")

    def downStreamGateway(df: DataFrame, batchID: Long): Unit = {
      df.persist()

      //      df.selectExpr("customer_id AS CUSTOMER_ID",
      //          "subsidiary AS SUBSIDIARY",
      //          "login_date AS LOGIN_DATE",
      //          "login_country_code AS LOGIN_COUNTRY_CODE",
      //          "app_device_id AS APP_DEVICE_ID")
      //        .repartition(1)
      //        .write
      //        .format("delta")
      //        .mode("append")
      //        .saveAsTable(config.getString("app.anomalyResultTablePath"))

      df.filter($"subsidiary"
          .equalTo("CDIP-CUB-DAT-R-LOGIN"))
        .selectExpr("customer_id AS key",
          s"to_json(struct(${subsidiaryOutputFields})) AS value")
        .write
        .format("kafka")
        .options(kafkaParams)
        .option("topic", "CDIP-CUB-ALT-R-NOTIFY")
        .save()

      df.filter($"subsidiary"
          .equalTo("CDIP-CXL-DAT-R-LOGIN"))
        .selectExpr("customer_id AS key",
          s"to_json(struct(${subsidiaryOutputFields})) AS value")
        .write
        .format("kafka")
        .options(kafkaParams)
        .option("topic", "CDIP-CXL-ALT-R-NOTIFY")
        .save()

      df.filter($"subsidiary"
          .equalTo("CDIP-SEC-DAT-R-LOGIN"))
        .selectExpr("customer_id AS key",
          s"to_json(struct(${subsidiaryOutputFields})) AS value")
        .write
        .format("kafka")
        .options(kafkaParams)
        .option("topic", "CDIP-SEC-ALT-R-NOTIFY")
        .save()

      df.unpersist()
    }

    processedStream.toDF().writeStream.foreachBatch(downStreamGateway _)
      .trigger(Trigger.ProcessingTime(config.getString("app.streamTriggerProcessingTime")))
      .outputMode("update")
      .option("checkpointLocation", config.getString("app.checkpointLocation"))
      .start()
      .awaitTermination()
  }
}
