package org.example

import org.apache.spark.sql.Row
import org.apache.spark.sql.streaming.GroupState
import org.bson.Document

import java.sql.Timestamp
import scala.collection.mutable

case class OutputAnomaly(userId: String,
                         loginTime: Timestamp,
                         locationEng: String
                        )

case class UserLocationInner(deviceType: mutable.LinkedHashMap[String, Long],
                             country: mutable.LinkedHashMap[String, Long])

case class UserLocation(uuidData: mutable.Map[String, UserLocationInner])

object UserStateHandler {

  def updateState(
                   userId: String,
                   inputs: Iterator[Row],
                   state: GroupState[UserLocation]
                 ): Iterator[String] = {

    var currentState = state.getOption

    // Collect all alerts
    val alerts = scala.collection.mutable.ListBuffer[String]()

    // Process each value in the batch
    inputs.foreach { row =>
      currentState match {
        case Some(prevLocation) =>
          //val timeDiff = row.getAs[Timestamp]("loginTime").getTime - prevLocation.loginTime.getTime
          //if (timeDiff <= 3600 * 1000 && row.getAs[String]("locationEng") != prevLocation.locationEng) {
          // User moved cities within 1 hour
          //   alerts += s"Alert: User $userId moved from ${prevLocation.locationEng} to ${row.getAs[String]("locationEng")}"
          // }
          val userLocB = prevLocation.uuidData.getOrElseUpdate(
            row.getAs[String]("deviceId"),
            UserLocationInner(mutable.LinkedHashMap(), mutable.LinkedHashMap())
          )
          userLocB.deviceType += (row.getAs[String]("loginType") -> row.getAs[Timestamp]("loginTime").getTime)
          userLocB.country += (row.getAs[String]("locationEng") -> row.getAs[Timestamp]("loginTime").getTime)

        case None =>
          // Initialize the state with the first location
          currentState = Some(UserLocation(mutable.Map(row.getAs[String]("deviceId") -> UserLocationInner(
            mutable.LinkedHashMap(row.getAs[String]("loginType") -> row.getAs[Timestamp]("loginTime").getTime),
            mutable.LinkedHashMap(row.getAs[String]("locationEng") -> row.getAs[Timestamp]("loginTime").getTime)
          ))))
      }
    }
    println(userId + " " + currentState)
    // Update the state with the last location in the batch
    currentState.foreach(state.update)

    // Set the timeout to 1 hour to clean up stale states
    state.setTimeoutDuration("10 seconds")

    // Check if the state is timed out
    if (state.hasTimedOut) {
      // Remove the state
      state.remove()
    }

    alerts.iterator
  }
}
