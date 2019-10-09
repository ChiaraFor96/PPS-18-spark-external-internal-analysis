package structuredStreaming

object StreamingDataOrganizerUtils {

  import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
  import org.apache.spark.sql.functions.length
  import org.apache.spark.sql.{Dataset, SparkSession}
  import structuredManipulationUtilities._

  val spark: SparkSession = SparkSession.builder.appName ( "Structured Streaming" )
    .master ( "local[*]" )
    .getOrCreate
  spark.sparkContext.setLogLevel ( "ERROR" )

  import spark.implicits._

  val valueColumn: DatasetColumn = DatasetColumn ( "value" )
  val timestampColumn: DatasetColumn = DatasetColumn ( "timestamp" )
  val lengthColumn: DatasetColumn = DatasetColumn ( "length" )
  val avgValueColumn: DatasetColumn = DatasetColumn ( "avgValue" )
  val maxLengthColumn: DatasetColumn = DatasetColumn ( "maxLength" )

  def getDataset ( rowsPerSecond: Int ): Dataset[StreamRecord] = {
    spark.readStream
      .format ( "rate" ) //produce data for testing
      .option ( "rowsPerSecond", rowsPerSecond )
      .load
      .withColumn ( lengthColumn.value, length ( valueColumn ) )
      // or .withColumn ( lengthColumn.value, udf { s: StreamType => s.toString.length }.apply ( value.column ) )
      //define your scala function with udf (but there is a problem.. serialization)
      .as [StreamRecord] //use case class and typing of SparkSQL, there's the difference between a dataset and a dataFrame
  }

  object structuredManipulationUtilities {

    import org.apache.spark.sql.Column

    type StreamValueType = Long

    case class DatasetColumn ( value: String ) extends Column ( Symbol(value).expr )

    case class StreamRecord ( value: StreamValueType, timestamp: Timestamp, length: Int )

    object ColumnOperations {
      def avgOfColumns ( columns: Column* ): Column = columns.reduce ( _.as [Int] + _ ) / columns.size

      def maxOfColumns ( columns: Column* ): Column = columns ( 0 ) // TODO map(_.as[Int]).max
    }

  }

}

/* FOR SOCKET
  val socketDF = spark.readStream
      .format ( "socket" )
      .options ( Map ( "host" -> "localhost", "port" -> "9999" ) )
      .load
 */