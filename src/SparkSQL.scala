import org.apache.spark.sql._

object SparkSQL extends App {
  val sc = SparkSettings()

  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  case class Cd(discId: String, age: Int)

  // Create an RDD of Person objects and register it as a table.
 /* val people = sc.textFile("examples/src/main/resources/people.txt").map(_.split(",")).map(p => Person(p(0), p(1).trim.toInt)).toDF()
  people.registerTempTable("people")

  // SQL statements can be run by using the sql methods provided by sqlContext.
  val teenagers = sqlContext.sql("SELECT name, age FROM people WHERE age >= 13 AND age <= 19")

  // The results of SQL queries are DataFrames and support all the normal RDD operations.
  // The columns of a row in the result can be accessed by field index:
  teenagers.map(t => "Name: " + t(0)).collect().foreach(println)

  // or by field name:
  teenagers.map(t => "Name: " + t.getAs[String]("name")).collect().foreach(println)

  // row.getValuesMap[T] retrieves multiple columns at once into a Map[String, T]
  teenagers.map(_.getValuesMap[Any](List("name", "age"))).collect().foreach(println)
  // Map("name" -> "Justin", "age" -> 19)*/
}
