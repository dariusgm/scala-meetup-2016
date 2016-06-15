package de.wlw.meetups.scala

object FileProcessing extends App {
  val sc = SparkSettings()
  val lines = sc.textFile("hdfs://192.168.99.100:9000/folk.csv")
  val out = List[Long](lines.count)
  val list =  sc.parallelize(out)
  list.saveAsTextFile("hdfs://192.168.99.100:9000/folk2.csv")
  print(lines.count)
}
