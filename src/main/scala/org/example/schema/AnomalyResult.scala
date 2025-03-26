package org.example.schema

import java.sql.Timestamp

case class AnomalyResult(customer_id: String,
                         subsidiary: String,
                         login_date: Timestamp,
                         login_country_code: String,
                         app_device_id: String,
                         is_anomaly_s: Boolean,
                         is_anomaly_cd: Boolean,
                         is_anomaly_d: Boolean,
                        )
