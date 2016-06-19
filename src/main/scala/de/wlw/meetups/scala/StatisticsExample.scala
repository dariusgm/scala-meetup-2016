package de.wlw.meetups.scala

import org.apache.spark.SparkContext
import org.apache.spark.mllib.random.RandomRDDs._
import org.apache.spark.mllib.stat.Statistics

object StatisticsExample extends App {
  import org.apache.spark.SparkConf
  val config = new SparkConf()
    .setAppName("LineCount")
    .setMaster("local[8]")
    .set("spark.executor.memory","2g")

  val sc = new SparkContext(config)
  val vectors = normalVectorRDD(sc,1000000L,2,8,123456)
  val summary =  Statistics.colStats(vectors)
  println("mean: " +summary.mean) // a dense vector containing the mean value for each column
  println("variance" + summary.variance)
  println("nonzeros" + summary.numNonzeros)
}
