package Optimize
import org.apache.spark.sql.SparkSession
import com.datastax.spark.connector.rdd.CassandraTableScanRDD
import com.datastax.spark.connector._

class optimizeApp {

  def shuffle(ss : SparkSession) = {
    /* InEfficient Shuffling */
    val purchasesRDD = ss.sparkContext.parallelize(List((1, 20), (1, 30), (1, 40), (2, 44), (2, 72), (3, 20), (4, 20), (4, 30), (4, 23), (5, 20), (5, 30), (5, 40), (5, 120), (5, 20), (1, 56)))
    val purchaseByCustomer = purchasesRDD.groupByKey().map(f => (f._1, (f._2.size, f._2.sum))).collect()
    purchaseByCustomer.foreach(f => println("Customer ID " + f._1 + ":" + " Size :" + f._2._1 + " Purchase :" + f._2._2))

    /* Efficient Shuffling */
    val purchasesRDD1 = ss.sparkContext.parallelize(List((1, 20), (1, 30), (1, 40), (2, 44), (2, 72), (3, 20), (4, 20), (4, 30), (4, 23), (5, 20), (5, 30), (5, 40), (5, 120), (5, 20), (1, 56)))
    val purchaseByCustomer1 = purchasesRDD1.map(f => (f._1, (1, f._2))).reduceByKey((v1, v2) => (v1._1 + v2._1, v1._2 + v2._2)).collect()
    purchaseByCustomer1.foreach(f => println("Customer ID " + f._1 + ":" + " Size :" + f._2._1 + " Purchase :" + f._2._2))
  }
}

/*

class dsExlpore {

  def explore(rdd : CassandraTableScanRDD): Unit = {

    /* Data Exploration */
    println(rdd.map(f => (f.getString("gender"), (f.getInt("age"), f.getInt("heart_disease")))).groupByKey().map(f => (f._1, f._2.size)).take(3))

    /* Heart Disease by Group */
    println(rdd.map(f => (f.getString("gender"), f.getInt("heart_disease"))).groupByKey().map(f => (f._1, f._2.sum)).take(3))

    /* more Efficient */
    rdd.map(f => (f.getString("gender"), (1, f.getInt("heart_disease")))).reduceByKey((v1, v2) => (v1._1 + v2._1, v1._2 + v2._2)).take(3)
  }

}

 */