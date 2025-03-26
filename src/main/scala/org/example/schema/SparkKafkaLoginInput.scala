package org.example.schema

import org.apache.spark.sql.types.{StructType, StructField, StringType, TimestampType}

// Use Spark Structure Datatype
object SparkKafkaLoginInput {

  def getSchema: StructType = {
    StructType(Seq(
      StructField("login_date", TimestampType),
      //StructField("login_time", StringType),
      StructField("login_status", StringType),
      //StructField("login_channel", StringType),
      StructField("login_country_code", StringType),
      StructField("app_device_id", StringType),
      StructField("model", StringType),
      //StructField("os_type_version", StringType),
      StructField("web_device_id", StringType),
      //StructField("web_os_type_version", StringType),
      //StructField("browser", StringType),
      //StructField("ip", StringType),
      //StructField("os_type_code", StringType),
      //StructField("country_desc", StringType),
      //StructField("os_version", StringType)
    ))
  }
}
