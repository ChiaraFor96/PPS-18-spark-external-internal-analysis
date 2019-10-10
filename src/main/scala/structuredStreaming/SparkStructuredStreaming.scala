package structuredStreaming

object SparkStructuredStreaming {
  def main ( args: Array[String] ): Unit = {
    import StreamingDataOrganizerUtils._
    import StreamingDataOrganizerUtils.structuredManipulationUtilities._
    import StreamingDataOrganizerUtils.structuredManipulationUtilities.ColumnOperations._
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.streaming.Trigger

    val dataset1 = getDataset ( 10 )
    val dataset2 = getDataset ( 20 )
    val dataset3 = getDataset ( 30 )

    //udf function
    dataset1.select ( udf { s: Long => s.hashCode }.apply( idColumn ).as ( "hash code" ) )
      .writeStream
      .trigger ( Trigger.ProcessingTime ( "2 seconds" ) )
      //.trigger ( Trigger.Continuous ( "5 seconds" ) ) //use experimental CP (in Spark 2.3+).. don't work :)
      .format ( "console" )
      .start

    //join 3 Datasets
    val datasets = dataset1.join ( dataset2.toDF, idColumn.name )
      .join ( dataset3, idColumn.name )
      .withColumn ( avgValueColumn.name,
      avgOfColumns (dataset1.col ( valueColumn.name ), dataset2.col ( valueColumn.name ), dataset3.col ( valueColumn.name ) ) )
  /*.withColumn ( maxLengthColumn.name,
       maxOfColumns ( dataset1.col ( lengthColumn.name )+ dataset2.col ( lengthColumn.name ) + dataset3.col ( lengthColumn.name )  ))*/
    datasets//.drop ( datasets.columns.filter ( _.equals ( valueColumn.name ) ): _* )
      //.drop ( datasets.columns.filter ( _.equals ( lengthColumn.name ) ): _* )
      //.withColumnRenamed(avgValueColumn.value, valueColumn.value)
      /* .withWatermark ( timestampColumn.value, "5 seconds")
       .groupBy (
         window ( timestampColumn.column, "10 seconds", "5 seconds" ),
         dataset1.col(lengthColumn.value)) //TODO don't work
       .count*/
      .select ( "*" )
      .writeStream
      .format ( "console" )
      .start

    //await any computation
    spark.streams.awaitAnyTermination
  }

  //TODO refactor seconds!!!
}
