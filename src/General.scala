import org.apache.spark._
object General extends App {
  val path = "C:/spark/folk.csv"
  val config = new SparkConf()
      .setAppName("LineCount")
      .setMaster("local[4]")
      .set("spark.executor.memory","1g")

  val sc =  new SparkContext(config)
  val lines = sc.textFile(path)
  print(lines.count)
}
