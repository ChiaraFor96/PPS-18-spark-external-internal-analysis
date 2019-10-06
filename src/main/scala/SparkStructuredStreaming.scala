import org.apache.spark.sql.streaming.Trigger

object SparkStructuredStreaming {

  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
  import org.apache.spark.sql.functions._

  def main ( args: Array[String] ): Unit = {
    //val log = org.apache.log4j.Logger.getLogger ( getClass.getName )

    val spark = SparkSession.builder ().appName ( "Quick start" )
      .master ( "local[*]" )
      .getOrCreate
    spark.sparkContext.setLogLevel ( "ERROR" )

    /*val socketDF = spark.readStream
      .format ( "socket" )
      .options ( Map ( "host" -> "localhost", "port" -> "9999" ) )
      .load
      */
    val socketDF1 = spark.readStream
      .format ( "rate" ) //produce data for testing
      .option ( "rowsPerSecond", 10 )
      .load
    val socketDF2 = spark.readStream.format ( "rate" ).option ( "rowsPerSecond", 10 ).load

    socketDF1.isStreaming
    socketDF1.printSchema
    import spark.implicits._

    val socketDS1 = socketDF1.as[(String, Timestamp)]
      .withColumn ( "length", length ( 'value ) )
      //define your scala function! with udf (but there is a problem.. serialization)
      .as [StreamRecord] //use case class and typing of SparkSQL

    //group by key and count
    socketDS1
      .withWatermark ( "timestamp", "10 seconds" )
      .groupBy (
        window ( $"timestamp", "10 seconds", "5 seconds" ),
        $"value" )
      .count
      .sort ( 'value )
      .writeStream
      .trigger ( Trigger.ProcessingTime ( "2 seconds" ) ) //Trigger fixed
      .outputMode ( "complete" )
      .format ( "console" )
      .start

    //udf function
    socketDS1.select ( udf { s: String => s.hashCode }.apply ( 'value ).as ( "myUDF" ) ).writeStream.format ( "console" ).start

    //join 2 Dataset
    val socketDS2 = socketDF2.as[(String, Timestamp)]
      .withColumn ( "length", udf { s: String => s.length }.apply ( 'value ) )
      .as [StreamRecord]
    socketDS1.join ( socketDF2, "value" ).select ( "*" ).writeStream.format ( "console" ).start

    //await any computation
    spark.streams.awaitAnyTermination
  }

  case class StreamRecord ( value: String, timestamp: Timestamp, length: Int )

}