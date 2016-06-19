package de.wlw.meetups.scala

import scala.reflect.io.File

object Streaming extends App {
  import org.apache.spark.{SparkConf}
  import org.apache.spark.streaming.{Seconds, StreamingContext}


  val config = new SparkConf()
    .setAppName("LineCount")
    .setMaster("local[8]")
    .set("spark.executor.memory","2g")

  val ssc = new StreamingContext(config, Seconds(10))
  val inputDirectory = "input"
  val lines = ssc.textFileStream(inputDirectory)
  val words = lines.flatMap(_.split(" "))
  val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
  print(wordCounts.print())
  ssc.start()
  ssc.awaitTermination()
}

object FileGenerator extends App {
  (1 to 5).foreach(counter => {
    val string = "a" * counter
    scala.tools.nsc.io.File("input" + File .separator + counter + ".txt").writeAll(string)
   Thread.sleep(counter * 1000)
  })
}