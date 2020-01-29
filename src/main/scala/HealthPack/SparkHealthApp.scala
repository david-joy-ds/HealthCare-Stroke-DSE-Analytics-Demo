package HealthPack

import org.apache.spark.sql.SparkSession
import com.datastax.spark.connector._

object SparkHealthApp extends App {

    val ss = SparkSession.builder                 /*Create Spark session */
      .appName("HealthAnalyticsApp")
      .enableHiveSupport()
      .getOrCreate()

    val rdd = ss.sparkContext.cassandraTable("keyspace1","health_stroke")
    println(rdd.count)

    ss.stop()   /* Session Stop */
}
