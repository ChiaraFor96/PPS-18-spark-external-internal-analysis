package structuredStreaming

object SparkStructuredStreaming {
  def main ( args: Array[String] ): Unit = {
    import StreamingDataOrganizerUtils._
    import StreamingDataOrganizerUtils.structuredManipulationUtilities.ColumnOperations._
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.streaming.Trigger

    val dataset1 = getDataFrame ( 10 )
    val dataset2 = getDataFrame ( 20 )
    val dataset3 = getDataFrame ( 30 )

    //udf function
    dataset1.select ( udf { s: Long => s.hashCode }.apply ( idColumn ).as ( "hash code" ) )
      .writeStream
      .trigger ( Trigger.ProcessingTime ( "2 seconds" ) )
      //.trigger ( Trigger.Continuous ( "5 seconds" ) ) //use experimental CP (in Spark 2.3+).. don't work :)
      .format ( "console" )
      .start

    //join 3 Datasets
    val datasets = dataset1.join ( dataset2.toDF, idColumn.name )
      .join ( dataset3, idColumn.name )
      .withColumn ( avgValueColumn.name,
        avgOfColumns ( dataset1.col ( valueColumn.name ), dataset2.col ( valueColumn.name ), dataset3.col ( valueColumn.name ) ) )
      .withColumn ( avgTimestampColumn.name,
        to_timestamp(avgOfColumns ( unix_timestamp(dataset1.col ( timestampColumn.name )), unix_timestamp(dataset2.col ( timestampColumn.name )), unix_timestamp(dataset3.col ( timestampColumn.name ) ))) )

    datasets.drop ( datasets.columns.filter ( _.equals ( valueColumn.name ) ): _* )
      .drop ( datasets.columns.filter ( _.equals ( timestampColumn.name ) ): _* )
      .withWatermark ( avgTimestampColumn.name, "5 seconds")
      .groupBy (window ( avgTimestampColumn, "10 seconds", "5 seconds" ))
      .mean(avgValueColumn.name)
      .writeStream
      .format ( "console" )
      .start

    spark.streams.awaitAnyTermination
  }

  //TODO refactor seconds!!!
}
