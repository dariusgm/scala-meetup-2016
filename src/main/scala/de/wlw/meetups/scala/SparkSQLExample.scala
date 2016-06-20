package de.wlw.meetups.scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

case class FreeDB(discId: String, discTitle: String, year: String, genre: String, titles: String)

object SparkSQLExample extends App {
  val config = new SparkConf()
    .setAppName("LineCount")
    .setMaster("local[8]")
    .set("spark.executor.memory","2g")

  val sc = new SparkContext(config)

  // Convert
  val lines = sc.textFile("folk.csv").cache()
  val mappedResult = lines.map(line => {
    val splitted = line.split(",")
    val discId = splitted(0)
    val discTitle = splitted(1)
    val year = splitted(2)
    val genre = splitted(3)
    val titles = splitted(4)
    FreeDB(discId,discTitle,year,genre,titles)
  })


  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._

  val df = mappedResult.toDF()

  println(df.show())
  println(df.printSchema())

  df.filter(df("year") === "1987").show()
  df.filter((df("genre") === "folk").and(df("year") === "1987")).show()
}
