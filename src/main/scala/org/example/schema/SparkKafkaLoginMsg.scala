package org.example.schema

import org.apache.spark.sql.types.{StructType, StructField, StringType, TimestampType}

// Use Spark Structure Datatype
object SparkKafkaLoginMsg {

  def getSchema(): StructType = {
    StructType(Seq(
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
  }
}
