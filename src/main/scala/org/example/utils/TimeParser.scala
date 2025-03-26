package org.example.utils

import scala.util.matching.Regex

object TimeParser {
  val timeUnits: Map[String, Long] = Map(
    "second" -> 1000,
    "minute" -> 60 * 1000,
    "hour" -> 60 * 60 * 1000,
    "day" -> 24 * 60 * 60 * 1000
  )

  // Regex to match the number and unit from the string
  val timePattern: Regex = """(\d+)\s*(\w+)""".r

  def parseTimeExpr(timeStr: String): Long = {
    // Trim the input and match the pattern
    timeStr.trim.toLowerCase match {
      case timePattern(numberStr, unit) =>
        // Convert the number to an integer
        val number = numberStr.toInt

        // Handle plural units (e.g., "minutes" -> "minute")
        val unitWithoutPlural = if (unit.endsWith("s")) unit.dropRight(1) else unit

        // Check if the unit is valid and get its corresponding value in milliseconds
        timeUnits.get(unitWithoutPlural) match {
          case Some(unitValue) => number * unitValue
          case None => throw new IllegalArgumentException(s"Unsupported time unit: $unit")
        }

      case _ => throw new IllegalArgumentException("Invalid time expression format")
    }
  }
}