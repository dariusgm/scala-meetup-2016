
object File extends App {
 val sc = SparkSettings()
  val lines = sc.textFile("C:/spark/folk.csv")
  print(lines.count)
}
