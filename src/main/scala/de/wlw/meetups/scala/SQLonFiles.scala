package de.wlw.meetups.scala

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object SQLonFiles extends App {
  val config = new SparkConf()
  .setAppName("LineCount")
  .setMaster("local[8]")
  .set("spark.executor.memory","2g")

  val sc = new SparkContext(config)
  val sqlContext = new SQLContext(sc)
  val df = sqlContext.sql("SELECT * FROM csv.`folk.csv`")
  df.printSchema()
}
