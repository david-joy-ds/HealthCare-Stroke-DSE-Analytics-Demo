
import HealthPack.SparkHealthApp.{rdd, ss}
import org.apache.spark.sql.SparkSession
import com.datastax.spark.connector._

  val ss = SparkSession.builder                 /*Create Spark session */
    .master("local")
    .appName("HealthAnalyticsApp")
    .enableHiveSupport()
    .getOrCreate()

  val rdd = ss.sparkContext.cassandraTable("keyspace1","health_stroke")
  println(rdd.count)

  val purchasesRDD = ss.sparkContext.parallelize(List((1,20),(1,30),(1,40),(2,44),(2,72),(3,20),(4,20),(4,30),(4,23),(5,20),(5,30),(5,40),(5,120),(5,20),(1,56)))
  val purchaseByCustomer = purchasesRDD.groupByKey().map(f => (f._1,(f._2.size,f._2.sum))).collect()
  purchaseByCustomer.foreach(f => println("Customer ID "+f._1+":"+" Size :"+f._2._1+" Purchase :"+f._2._2))

/* Efficient Shuffling */
val purchasesRDD = ss.sparkContext.parallelize(List((1,20),(1,30),(1,40),(2,44),(2,72),(3,20),(4,20),(4,30),(4,23),(5,20),(5,30),(5,40),(5,120),(5,20),(1,56)))
val purchaseByCustomer = purchasesRDD.map(f => (f._1,(1,f._2))).reduceByKey((v1,v2) => (v1._1 + v2._1, v1._2 + v2._2)).collect()
purchaseByCustomer.foreach(f => println("Customer ID "+f._1+":"+" Size :"+f._2._1+" Purchase :"+f._2._2))


val data = spark.read.cassandraFormat("health_stroke","keyspace1").load()
data.describe().select("gender","age","avg_glucose_level","bmi","heart_disease","hypertension","residence_type","stroke","work_type").show()
data.filter ($"gender" =!= "null").show(

val data = spark.read.cassandraFormat("health_stroke","keyspace1").load()
val Formated_data = data.selectExpr("cast(gender as int) as gender","age","avg_glucose_level","bmi","cast(ever_married as int) as ever_married","heart_disease","hypertension","stroke")
Formated_data.printSchema
val df = Formated_data.toDF

/* InEfficient Shuffling */
val purchasesRDD = ss.sparkContext.parallelize(List((1,20),(1,30),(1,40),(2,44),(2,72),(3,20),(4,20),(4,30),(4,23),(5,20),(5,30),(5,40),(5,120),(5,20),(1,56)))
val purchaseByCustomer = purchasesRDD.groupByKey().map(f => (f._1,(f._2.size,f._2.sum))).collect()
purchaseByCustomer.foreach(f => println("Customer ID "+f._1+":"+" Size :"+f._2._1+" Purchase :"+f._2._2))

/* Efficient Shuffling */
val purchasesRDD = ss.sparkContext.parallelize(List((1,20),(1,30),(1,40),(2,44),(2,72),(3,20),(4,20),(4,30),(4,23),(5,20),(5,30),(5,40),(5,120),(5,20),(1,56)))
val purchaseByCustomer = purchasesRDD.map(f => (f._1,(1,f._2))).reduceByKey((v1,v2) => (v1._1 + v2._1, v1._2 + v2._2)).collect()
purchaseByCustomer.foreach(f => println("Customer ID "+f._1+":"+" Size :"+f._2._1+" Purchase :"+f._2._2))

/* Data Exploration */
println(rdd.map(f => (f.getString("gender"),(f.getInt("age"),f.getInt("heart_disease")))).groupByKey().map(f => (f._1,f._2.size)).take(3))

/* Heart Disease by Group */
println(rdd.map(f => (f.getString("gender"),f.getInt("heart_disease"))).groupByKey().map(f => (f._1,f._2.sum)).take(3))

/* more Efficient */
rdd.map(f => (f.getString("gender"),(1,f.getInt("heart_disease")))).reduceByKey((v1,v2) => (v1._1+v2._1,v1._2+v2._2)).take(3)


)
