object SparkStreaming {
  import org.apache.spark.SparkConf
  import org.apache.spark.streaming.{Seconds, StreamingContext}

  def main ( args: Array[String] ): Unit = {
    val ssc = new StreamingContext(new SparkConf().setAppName("Spark streaming example").setMaster("local[*]"), Seconds(2))
    val s1 = ssc.socketTextStream("localhost", 9999) //started with nc -l -p 9998 localhost
    val s2 = ssc.socketTextStream("localhost", 9998)
    s1.map(x => s"Sensor1: $x").print
    s2.map(x => s"Sensor2: $x").print
    ssc.start()
    ssc.awaitTermination()
  }
}
