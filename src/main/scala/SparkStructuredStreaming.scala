object SparkStructuredStreaming {

  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.functions._

  case class Word(value : String, length : Int)
  def main ( args: Array[String] ): Unit = {
    //val log = org.apache.log4j.Logger.getLogger ( getClass.getName )

    val spark = SparkSession.builder ().appName ( "Quick start" )
      .master ( "local[*]" )
      .getOrCreate
    spark.sparkContext.setLogLevel("ERROR")

    val socketDF = spark.readStream
      .format ( "socket" )
      .options ( Map ( "host" -> "localhost", "port" -> "9999" ) )
      .load

    socketDF.isStreaming
    socketDF.printSchema
    import spark.implicits._

    val socketDS = socketDF.as[String]
      .flatMap ( _.split ( " " ) )
      .withColumn("length", length('value))
      //define your scala function! with udf (but there is a problem.. serialization)
      .as[Word] //use case class and typing of SparkSQL

    //group by key and count
    socketDS
      .groupByKey(_.length)
      .count
      .sort('value)
      .writeStream
      .outputMode ( "complete" )
      .format ( "console" )
      .start
    // TODO use a streaming with timestamp, try window and watermark, use sample sources for simulate?
    // TODO can I use some like the receiver in SparkStreaming for simulate input data?

    val fun : String => Int = _(0)
    val udfFun = udf(fun)

    socketDS.select(udfFun('value).as("myUDF")).writeStream.format ( "console" ).start

    //await any computation
    spark.streams.awaitAnyTermination
  }
}