import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, Dataset}

object SparkStructuredStreaming {

  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
  import org.apache.spark.sql.functions._

  type StreamType = Long
  case class StreamRecord ( value: StreamType, timestamp: Timestamp, length: Int )

  def main ( args: Array[String] ): Unit = {
    //val log = org.apache.log4j.Logger.getLogger ( getClass.getName )

    val spark = SparkSession.builder.appName ( "Structured Streaming" )
      .master ( "local[*]" )
      .getOrCreate
    spark.sparkContext.setLogLevel ( "ERROR" )

    def getRateDataFrame: DataFrame = {
      spark.readStream
        .format ( "rate" ) //produce data for testing
        .option ( "rowsPerSecond", 10 )
        .load
    }

    /*val socketDF = spark.readStream
          .format ( "socket" )
          .options ( Map ( "host" -> "localhost", "port" -> "9999" ) )
          .load
          */
    val dataFrame1 = getRateDataFrame
    val dataFrame2 = getRateDataFrame

    dataFrame1.isStreaming
    dataFrame1.printSchema
    import spark.implicits._

    def getDataset ( dataFrame: DataFrame ): Dataset[StreamRecord] = {
      dataFrame.as[(StreamType, Timestamp)]
        .withColumn ( "length", length ( 'value ) )
        // or .withColumn ( "length", udf { s: StreamType => s.toString.length }.apply ( col("value") ) )
        //define your scala function with udf (but there is a problem.. serialization)
        .as [StreamRecord] //use case class and typing of SparkSQL
    }

    val dataset1 = getDataset ( dataFrame1 )
    val dataset2 = getDataset ( dataFrame2 )

    //group by key and count
    dataset1
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
    dataset1.select ( udf { s: StreamType => s.hashCode }.apply ( 'value ).as ( "hash code" ) ).writeStream.format ( "console" ).start

    //join 2 Dataset
    dataset1.join ( dataset2, "value" ).select ( "*" ).writeStream.format ( "console" ).start

    //await any computation
    spark.streams.awaitAnyTermination
  }
}