package de.wlw.meetups.scala



/**
  * Created by D on 17.06.2016.
  */
object Streaming extends App {
  import org.apache.spark.storage.StorageLevel
  import org.apache.spark.{SparkConf}
  import org.apache.spark.streaming.{Seconds, StreamingContext}

  val config = new SparkConf()
    .setAppName("LineCount")
    .setMaster("local[8]")
    .set("spark.executor.memory","2g")

  // val sc = new SparkContext(config)
  val ssc = new StreamingContext(config, Seconds(2))
  val rawStreams = (1 to 2).map(_ =>
    ssc.rawSocketStream[String]("localhost", 60000, StorageLevel.MEMORY_ONLY_SER_2)).toArray
  val union = ssc.union(rawStreams)
  union.filter(_.contains("the")).count().foreachRDD(r =>
    println("Grep count: " + r.collect().mkString))
  ssc.start()
  ssc.awaitTermination()
}


object StreamingClient extends App {
  import scalaj.http._

  val response: HttpResponse[String] = Http("http://localhost:6000").param("q","monkeys").asString
}