package de.wlw.meetups.scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object DataSetsExample extends App {
  val config = new SparkConf()
    .setAppName("LineCount")
    .setMaster("local[8]")
    .set("spark.executor.memory","2g")

  val sc = new SparkContext(config)
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._

  val list = (1 to 1000000).toList
  val dataSet = list.toDS()
  dataSet.map(_ * 2).take(100).foreach(println)

}
