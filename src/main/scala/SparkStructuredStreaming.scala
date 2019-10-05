object SparkStructuredStreaming {

  import org.apache.spark.sql.SparkSession

  case class Word(value : String)
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

    val result = socketDF.as[String]
      .flatMap ( _.split ( " " ) )
      .as[Word] //use case class and typing of SparkSQL
      .groupByKey(_.value)
      .count

    val query = result.writeStream
      .outputMode ( "complete" )
      .format ( "console" )
      .start

    query.awaitTermination
  }
}