package de.wlw.meetups.scala

import org.apache.spark.graphx.{Edge, Graph, GraphLoader, VertexId}
import org.apache.spark.rdd.RDD

object GraphXExample extends App {
  import org.apache.spark.SparkConf
  import org.apache.spark.SparkContext

  val config = new SparkConf()
    .setAppName("LineCount")
    .setMaster("local[8]")
    .set("spark.executor.memory","2g")

  val sc = new SparkContext(config)
  val graph = GraphLoader.edgeListFile(sc, "followers.txt")
  // Run PageRank
  val ranks = graph.pageRank(0.0001).vertices
  // Join the ranks with the usernames
  val users = sc.textFile("users.txt").map { line =>
    val fields = line.split(",")
    (fields(0).toLong, fields(1))
  }
  val ranksByUsername = users.join(ranks).map {
    case (id, (username, rank)) => (username, rank)
  }
  // Print the result
  ranksByUsername.map(_.swap).top(7).foreach(println)
}
