import org.apache.spark.{SparkConf, SparkContext}

object SparkSettings {
  def apply(): SparkContext = {
    val config = new SparkConf()
      .setAppName("LineCount")
      .setMaster("local[4]")
      .set("spark.executor.memory","1g")

    new SparkContext(config)
  }

}
