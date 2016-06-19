package de.wlw.meetups.scala

import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.random.RandomRDDs._

object ClusteringExample extends App {
  import org.apache.spark.SparkConf
  import org.apache.spark.SparkContext

  val config = new SparkConf()
    .setAppName("LineCount")
    .setMaster("local[8]")
    .set("spark.executor.memory","2g")

  val sc = new SparkContext(config)
  val vectors = normalVectorRDD(sc, 1000000L ,2 ,8 , 123456)
  val numClusters = 2
  val numIterations = 20
  val clusters = KMeans.train(vectors, numClusters,numIterations)

  clusters.clusterCenters.foreach(vector => {
    vector.toArray.map(_.toString).foreach(println)
    }
  )
}
