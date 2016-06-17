package de.wlw.meetups.scala

import org.apache.spark.{SparkConf, SparkContext}

object FileProcessing extends App {
  val config = new SparkConf()
    .setAppName("LineCount")
    .setMaster("local[8]")
    .set("spark.executor.memory","2g")

  val sc = new SparkContext(config)
  val lines = sc.textFile("folk.csv").cache()

  // First Entry in Line
  val firstLine = lines.first() // Action
  println("firstline: " + firstLine) // cd11750f,Broery Marantika / The best of,,,hati yang terluka aku jatuh cinta aku beg

  val splittedAlbum = lines.map(line => line.split(",")(1)).cache()
  val firstAlbum = splittedAlbum.first()
  println("firstalbum: " + firstAlbum)

  val flatmap = splittedAlbum.flatMap(_.split(" "))
  println("flatmap: " + flatmap)

  val words = flatmap.flatMap(line => line.split(" "))
    .map(word => (word, 1))
    .reduceByKey(_ + _)

  val sortByValues = words.map((item) => item.swap).sortByKey(false)
  sortByValues.take(200).map(println)
}
