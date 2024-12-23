package org.example.schema

import java.sql.Timestamp

case class OutputAnomaly(userId: String,
                         loginTime: Timestamp,
                         prev_locationEng: String,
                         locationEng: String,
                         prev_deviceId: String,
                         deviceId: String
                        )
