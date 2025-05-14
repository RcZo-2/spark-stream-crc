package org.example

import org.example.util.TimestampUtils
import org.quartz.CronExpression

/**
 * Main program demonstrating batch processing with cron scheduling
 */
object TestQuartz {
  def main(args: Array[String]): Unit = {
    val cronStr = "0 0 16 * * ? *"  // Quartz cron format

    // Parse cron expression
    val cronExpr = new CronExpression(cronStr)

    // Simulate storing the watermark (i.e., last scheduled batch time) as UTC timestamp (milliseconds)
    var watermark: Long = System.currentTimeMillis() - 3600000 // current time minus 1 hour in millis

    // Simulated list of input timestamps (in UTC milliseconds)
    val inputTimestamps = TimestampUtils.createTestTimestamps()

    for (inputTime <- inputTimestamps) {
      println(s"\n📥 Input Time     : ${TimestampUtils.formatTimestamp(inputTime)}")
      println(s"📌 Current Watermark: ${TimestampUtils.formatTimestamp(watermark)}")

      // Compute the next scheduled batch time after the watermark
      val nextScheduledTime = TimestampUtils.getNextScheduledTime(cronExpr, watermark)
      println(s"🔜 Next Scheduled Batch After Watermark: ${TimestampUtils.formatTimestamp(nextScheduledTime)}")

      // Check if input is after the next scheduled batch time
      if (inputTime > nextScheduledTime) {
        println("✅ NEW BATCH DETECTED: Input is after next scheduled batch time.")
        watermark = inputTime  // Update watermark to latest input time
      } else {
        println("❌ SAME BATCH: Input is NOT after next scheduled batch time.")
      }
    }

    println("\n🏁 Final Watermark: " + TimestampUtils.formatTimestamp(watermark))
  }
}
