package structuredStreaming

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.length

object DatasetManipulationUtils {

  import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
  import org.apache.spark.sql.functions.col
  import org.apache.spark.sql.{Column, SparkSession}

  type StreamValueType = Long
  val valueColumn = DatasetColumn ( "value" )
  val timestampColumn = DatasetColumn ( "timestamp" )
  val lengthColumn = DatasetColumn ( "length" )
  val avgValueColumn = DatasetColumn ( "avgValue" )
  val maxLengthColumn = DatasetColumn ( "avgValue" )

  def getDataset ( spark: SparkSession, rowsPerSecond : Int): Dataset[StreamRecord] = {
    import spark.implicits._
    spark.readStream
      .format ( "rate" ) //produce data for testing
      .option ( "rowsPerSecond", rowsPerSecond )
      .load
      .withColumn ( lengthColumn.value, length ( valueColumn.column ) )
      // or .withColumn ( lengthColumn.value, udf { s: StreamType => s.toString.length }.apply ( value.column ) )
      //define your scala function with udf (but there is a problem.. serialization)
      .as [StreamRecord] //use case class and typing of SparkSQL, there's the difference between a dataset and a dataFrame
  }

  case class StreamRecord ( value: StreamValueType, timestamp: Timestamp, length: Int )

  case class DatasetColumn ( value: String, column: Column )

  object DatasetColumn {
    def apply ( value: String ): DatasetColumn = apply ( value, col ( value ) )
  }
}
/* FOR SOCKET
  val socketDF = spark.readStream
      .format ( "socket" )
      .options ( Map ( "host" -> "localhost", "port" -> "9999" ) )
      .load
 */