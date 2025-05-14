package org.example.util

import org.quartz.CronExpression
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId, ZonedDateTime}
import java.util.Date


object TimestampUtils {


  def formatTimestamp(timestamp: Long): String = {
    ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault())
      .format(DateTimeFormatter.ISO_DATE_TIME)
  }


  def getNextScheduledTime(cronExpr: CronExpression, afterTimestamp: Long): Long = {
    val date = new Date(afterTimestamp)
    val nextDate = cronExpr.getNextValidTimeAfter(date)
    nextDate.getTime // returns timestamp in milliseconds
  }


  def createTestTimestamps(): List[Long] = {
    val currentTimeMillis = System.currentTimeMillis()
    List(
      currentTimeMillis - 600000, // current time minus 10 minutes
      currentTimeMillis + 120000, // current time plus 2 minutes
      currentTimeMillis + 82800000, // current time plus 23 hours
      currentTimeMillis + 198000000 // current time plus 55 hours
    )
  }
}
