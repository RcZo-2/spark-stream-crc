package org.example

import org.apache.spark.sql.Row
import org.apache.spark.sql.streaming.GroupState
import com.mongodb.client.model.Filters
import com.mongodb.client.{MongoClient, MongoClients, MongoCollection, MongoDatabase}
import org.bson.Document

import java.sql.Timestamp
import scala.collection.mutable

case class OutputAnomaly(userId: String,
                         loginTime: Timestamp,
                         prev_locationEng: String,
                         locationEng: String,
                         prev_deviceId: String,
                         deviceId: String
                        )

object UserStateHandler {
  private final val mongoClient: MongoClient = MongoClients.create(s"${Config.mongoUri}")
  private final val database: MongoDatabase = mongoClient.getDatabase(s"${Config.mongoDB}")
  private final val collection: MongoCollection[Document] = database.getCollection(s"${Config.mongoColl}")

  def updateState(userId: String, inputs: Iterator[Row], state: GroupState[mutable.Map[String, (Timestamp, String, String)]]): Iterator[OutputAnomaly] = {

    val storeUserState = state.getOption.getOrElse(mutable.Map[String
      , (Timestamp, String, String)]())
    var outputData: mutable.Seq[OutputAnomaly] = mutable.Seq.empty

    // foreach input
    inputs.foreach { row =>
      val this_userId = row.getAs[String]("userId")
      val loginTime = row.getAs[Timestamp]("loginTime")
      val locationEng = row.getAs[String]("locationEng")
      val deviceId = row.getAs[String]("deviceId")

      //// MONGO Start ////
      val mongoCursor = collection.findOneAndDelete(Filters.eq("_id", this_userId))
      if (mongoCursor == null) {
        //println("Mongo get nothing")
      }
      else {
        storeUserState(userId) = (loginTime,
          mongoCursor.getString("locationEng"),
          mongoCursor.getString("deviceId"))
      }
      //// MONGO End ////


      /// State logic Start ///
      storeUserState.get(userId) match {
        case Some((prev_loginTime, prev_locationEng, prev_deviceId)) =>
          val timeDiffMs = (loginTime.getTime - prev_loginTime.getTime)
          val timeDiffSeconds = timeDiffMs.toDouble / 1000 // Convert milliseconds to seconds
          //println(userId, " ", timeDiffSeconds, " ", prev_locationEng, "->", locationEng, " ", prev_deviceId, "->", deviceId)

          if (timeDiffSeconds <= Config.appAnomalyTFraS && locationEng != prev_locationEng && deviceId != prev_deviceId) {
            outputData = outputData :+ OutputAnomaly(this_userId, loginTime, prev_locationEng, locationEng, prev_deviceId, deviceId)
          }
          storeUserState(userId) = (loginTime, locationEng, deviceId)

        case None =>
          //println(userId, " ", loginTime, " ", locationEng, " ", deviceId)
          storeUserState(userId) = (loginTime, locationEng, deviceId)
      }

      /// State logic End ///
    }
    // Batch update
    state.update(storeUserState)

    // Yields an iterator of anomalies
    val anomalies: Iterator[OutputAnomaly] = outputData.iterator
    anomalies
  }

}


