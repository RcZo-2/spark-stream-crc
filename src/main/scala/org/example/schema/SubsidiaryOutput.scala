package org.example.schema

// customer_id already at key
case class SubsidiaryOutput(subsidiary: String,
                            login_date: String,
                            login_country_code: String,
                            app_device_id: String,
                            is_anomaly_s: Boolean,
                            is_anomaly_cd: Boolean,
                            is_anomaly_d: Boolean
                           )

