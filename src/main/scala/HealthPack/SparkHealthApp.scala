package HealthPack

import org.apache.spark.sql.SparkSession
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._
import MLApp.mlflow
import Optimize.optimizeApp
import java.io._
/* import Optimize.dsExlpore */

object SparkHealthApp extends App {

    val ss = SparkSession.builder                 /*Create Spark session */
      .appName("HealthAnalyticsApp")
      .enableHiveSupport()
      .getOrCreate()

    /* testing data rdd */
    val rdd = ss.sparkContext.cassandraTable("keyspace1","health_stroke")
    println("Rdd count : "+rdd.count+"\n")

    println("Writing to health.parquet file ... \n")

    /* writing data to df */
    val dff = ss.sqlContext
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> "health_stroke", "keyspace" -> "keyspace1"))
      .load()

    println("df count ... \n"+dff.count())

    dff.write.parquet("file:///tmp/health.parquet")


    /* added this below 4 line code to check run as with spark */
    println("Writing to a File now .....\n")
    val writer = new PrintWriter(new File("test.txt"))
    writer.write("Run as Test, I am not broken")
    writer.close()

    /* setting up data for ML flow */
    val data = ss.read.cassandraFormat("health_stroke","keyspace1").load()
    val Formated_data = data.selectExpr("cast(gender as int) as gender","age","avg_glucose_level","bmi","cast(ever_married as int) as ever_married","heart_disease","hypertension","stroke")
    val df = Formated_data.toDF
    val df1 = df.withColumn("label",df("stroke")).drop("stroke")
    val mlflow =  new mlflow
    val ml = mlflow.LogisticR(df1)

    /* Data Exploration */
    val opt = new optimizeApp
    val op = opt.shuffle(ss)
    /* val dsexp = new dsExlpore
    val xplr = dsexp.explore(rdd) */

    ss.stop()   /* Session Stop */
}
