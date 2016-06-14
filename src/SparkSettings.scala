import java.io.{BufferedReader, InputStreamReader}

import org.apache.spark.{SparkConf, SparkContext}

object SparkSettings {
  def apply(): SparkContext = {
    val config = new SparkConf()
      .setAppName("LineCount")
      .setMaster("spark://" + ip + ":7077")
      .set("spark.executor.memory","1g")

    new SparkContext(config)
  }

  def ip = {
    val p = Runtime.getRuntime.exec("docker-machine ip")
    val stdInput = new BufferedReader(new
        InputStreamReader(p.getInputStream))
    // read the output from the command
    "192.168.99.100" //stdInput.readLine()
  }

}
