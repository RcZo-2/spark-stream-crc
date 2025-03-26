package org.example

import org.apache.log4j.Logger
import org.apache.spark.sql.Row
import org.apache.spark.sql.streaming.GroupState
import org.example.config.ConfigManager
import org.example.schema.AnomalyResult
import org.example.utils.TimeParser.parseTimeExpr

import java.sql.Timestamp
import scala.collection.mutable

object GroupLoginGuardLogic {
  private final val logger = Logger.getLogger(GroupLoginGuardLogic.getClass)

  def runAnomalyUserStateProcess(customer_id: String,
                                 inputs: Iterator[Row],
                                 state: GroupState[mutable.LinkedHashMap[(String, String, String), Long]]
                                ): Iterator[AnomalyResult] = {
    val config = ConfigManager.getConfig

    // Check if the state is timed out
    if (state.hasTimedOut) {
      // Remove the state
      state.remove()
    }
    // Read previous state
    var storedUserState = state.getOption


    // No need to process data that comes too late
    val ignoreTimeMs = state.getCurrentProcessingTimeMs() - parseTimeExpr(config.getString("app.msgIgnoreTime")) // 30 days
    val filteredInputs = inputs.filter(row => row.getAs[Timestamp]("login_date").getTime > ignoreTimeMs)

    // Sort in ascending, but only current batch
    val sortedInputs = filteredInputs.toList.sortBy(row => row.getAs[Timestamp]("login_date").getTime)

    // Assign output iterator
    var outputData: mutable.Seq[AnomalyResult] = mutable.Seq.empty

    // Set an expiration time for data deletion
    val whitelistExpireTimeMs = state.getCurrentProcessingTimeMs() - parseTimeExpr(config.getString("app.whitelistRetentionTime"))

    // Init an indicator for the usage of code that runs only once
    var isDataRefreshed = false

    sortedInputs.foreach { row =>
      val inputSubsidiary = row.getAs[String]("subsidiary")
      val inputLoginDate = row.getAs[Timestamp]("login_date").getTime //Unix Time (Long)
      val loginDateTs = row.getAs[Timestamp]("login_date")
      val inputLoginCountryCode = row.getAs[String]("login_country_code")
      val inputAppDeviceId = row.getAs[String]("app_device_id")
      val inputModel = row.getAs[String]("model")
      var isAnomalyS: Boolean = false
      var isAnomalyCD: Boolean = false
      var isAnomalyD: Boolean = false

      storedUserState match {
        case Some(loginHistMap) =>
          // Keep whitelist data only for those that have not expired, and do this once in single micro batch
          if (!isDataRefreshed) {
            loginHistMap.retain((keys, loginTime) => {
              loginTime > whitelistExpireTimeMs
            })
            isDataRefreshed = true
          }

          // TODO Logic start here
          isAnomalyS = !loginHistMap.exists {
            case ((histDevice, histModel, histCountry), histLoginTime) =>
              histDevice == inputAppDeviceId && histCountry == inputLoginCountryCode &&
                inputLoginDate - histLoginTime < parseTimeExpr(config.getString("app.anomalousMoveTimeThreshold"))
          }
          logger.debug(customer_id + " isAnomalyCaseS: " + isAnomalyS.toString)

          logger.debug(customer_id + " isAnomalyCaseCD: " + isAnomalyCD.toString)

          logger.debug(customer_id + " isAnomalyCaseD: " + isAnomalyD.toString)

          // Set new data in the map, remove it before adding to maintain the order
          loginHistMap.remove((inputAppDeviceId, inputModel, inputLoginCountryCode))
          loginHistMap.put((inputAppDeviceId, inputModel, inputLoginCountryCode), inputLoginDate)

        case None =>
          val initLoginTimelineMap = mutable.LinkedHashMap[(String, String, String), Long]()
          initLoginTimelineMap.put((inputAppDeviceId, inputModel, inputLoginCountryCode), inputLoginDate)
          storedUserState = Some(initLoginTimelineMap)
      }

      // Set output data return to stream
      outputData = outputData :+ AnomalyResult(
        customer_id,
        inputSubsidiary,
        loginDateTs,
        inputLoginCountryCode,
        inputAppDeviceId,
        isAnomalyS,
        isAnomalyCD,
        isAnomalyD)
    }

    // Save user state
    state.update(storedUserState.getOrElse(
      mutable.LinkedHashMap[(String, String, String), Long]())
    )
    state.setTimeoutDuration(config.getString("app.whitelistRetentionTime"))
    logger.debug(customer_id + " has state: " + storedUserState.toString)

    // Yields an iterator of anomalies
    val anomalies: Iterator[AnomalyResult] = outputData.iterator
    anomalies
  }
}
