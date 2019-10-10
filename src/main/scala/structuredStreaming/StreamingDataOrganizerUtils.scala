package structuredStreaming

object StreamingDataOrganizerUtils {

  import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.{Dataset, SparkSession}
  import structuredManipulationUtilities._

  val spark: SparkSession = SparkSession.builder.appName ( "Structured Streaming" )
    .master ( "local[*]" )
    .getOrCreate
  spark.sparkContext.setLogLevel ( "ERROR" )

  import spark.implicits._

  val idColumn: DatasetColumn = DatasetColumn ( "id" )
  val timestampColumn: DatasetColumn = DatasetColumn ( "timestamp" )
  val valueColumn: DatasetColumn = DatasetColumn ( "value" )
  val avgValueColumn: DatasetColumn = DatasetColumn ( "avgValue" )
  val maxLengthColumn: DatasetColumn = DatasetColumn ( "maxLength" )

  def getDataset ( rowsPerSecond: Int): Dataset[StreamRecord] = {
    //produce data for testing (can also define the schema)
    spark.readStream.format ( "rate" ).option ( "rowsPerSecond", rowsPerSecond ).load
      .withColumnRenamed("value", idColumn.name)
      .withColumn ( valueColumn.name, rand * 100)
      // or .withColumn ( valueColumn.name, udf { s: StreamType => ??? }.apply ( 'value ) )
      //define your scala function with udf (but there is a problem.. serialization)
      .as [StreamRecord] //use case class and typing of SparkSQL, there's the difference between a dataset and a dataFrame
  }

  object structuredManipulationUtilities {

    import org.apache.spark.sql.Column

    case class DatasetColumn ( name: String ) extends Column ( Symbol(name).expr )

    case class StreamRecord ( id: Long, timestamp: Timestamp, value: Double )

    object ColumnOperations {
      // TODO pass type
      def avgOfColumns ( columns: Column* ): Column = columns.reduce ( _.as [Double] + _ ) / columns.size

      def maxOfColumns ( columns: Column* ): Column = columns ( 0 ) // TODO map(_.as[T]).max
    }

  }

}

/* FOR SOCKET
  val socketDF = spark.readStream
      .format ( "socket" )
      .options ( Map ( "host" -> "localhost", "port" -> "9999" ) )
      .load
 */