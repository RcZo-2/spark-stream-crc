package org.example

import org.quartz.CronExpression

import java.time.format.DateTimeFormatter
import java.time.{ZoneId, ZonedDateTime}
import java.util.Date

object Test2 {
  def main(args: Array[String]): Unit = {
    val cronStr = "0 0 16 * * ? *"  // Quartz cron format

    // Parse cron expression
    val cronExpr = new CronExpression(cronStr)

    // Simulate storing the watermark (i.e., last scheduled batch time)
    var watermark: ZonedDateTime = ZonedDateTime.now().minusHours(1)

    // Helper to convert Java Date <-> ZonedDateTime
    def toZonedDateTime(date: java.util.Date): ZonedDateTime =
      ZonedDateTime.ofInstant(date.toInstant, ZoneId.systemDefault())

    // Function to get the next scheduled time after given time
    def getNextScheduledTime(after: ZonedDateTime): ZonedDateTime = {
      val nextDate = cronExpr.getNextValidTimeAfter(Date.from(after.toInstant))
      toZonedDateTime(nextDate)
    }

    // Simulated list of input timestamps
    val inputTimestamps = List(
      ZonedDateTime.now().minusMinutes(10),
      ZonedDateTime.now().plusMinutes(2),
      ZonedDateTime.now().plusHours(23),
      ZonedDateTime.now().plusHours(55)
    )

    for (inputTime <- inputTimestamps) {
      println(s"\n📥 Input Time     : ${inputTime.format(DateTimeFormatter.ISO_DATE_TIME)}")
      println(s"📌 Current Watermark: ${watermark.format(DateTimeFormatter.ISO_DATE_TIME)}")

      // Compute the next scheduled batch time after the watermark
      val nextScheduledTime = getNextScheduledTime(watermark)
      println(s"🔜 Next Scheduled Batch After Watermark: ${nextScheduledTime.format(DateTimeFormatter.ISO_DATE_TIME)}")

      // Check if input is after the next scheduled batch time
      if (inputTime.isAfter(nextScheduledTime)) {
        println("✅ NEW BATCH DETECTED: Input is after next scheduled batch time.")
        watermark = inputTime  // Update watermark to latest input time
      } else {
        println("❌ SAME BATCH: Input is NOT after next scheduled batch time.")
      }
    }

    println("\n🏁 Final Watermark: " + watermark.format(DateTimeFormatter.ISO_DATE_TIME))
  }
}
