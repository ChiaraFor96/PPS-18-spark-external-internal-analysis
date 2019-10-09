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
    /*dataset1.select ( udf { s: StreamValueType => s.hashCode }.apply( valueColumn ).as ( "hash code" ) )
      .writeStream
      //.trigger ( Trigger.Continuous ( "5 seconds" ) ) //use experimental CP (in Spark 2.3+).. don't work :)
      .format ( "console" )
      .start*/

    //join 3 Datasets
    val datasets = dataset1.join ( dataset2, timestampColumn.value )
      .join ( dataset3, timestampColumn.value )
      /*.withColumn ( avgValueColumn.value,
        avgOfColumns (dataset1.col ( valueColumn.value ), dataset2.col ( valueColumn.value ), dataset3.col ( valueColumn.value ) ) )
      .withColumn ( maxLengthColumn.value,
           maxOfColumns ( dataset1.col ( lengthColumn.value )+ dataset2.col ( lengthColumn.value ) + dataset3.col ( lengthColumn.value )  ))
*/
    datasets //.drop ( datasets.columns.filter ( _.equals ( valueColumn.value ) ): _* )
      // .drop ( datasets.columns.filter ( _.equals ( lengthColumn.value ) ): _* )
      //.withColumnRenamed(avgValueColumn.value, valueColumn.value)
      /* .withWatermark ( timestampColumn.value, "5 seconds")
       .groupBy (
         window ( timestampColumn.column, "10 seconds", "5 seconds" ),
         dataset1.col(lengthColumn.value)) //TODO don't work
       .count*/
      .select ( "*" )
      .writeStream
      .format ( "console" )
      .trigger ( Trigger.ProcessingTime ( "2 seconds" ) ) //fixed trigger, CP not supported in JOIN
      .start

    //await any computation
    spark.streams.awaitAnyTermination
  }

  //TODO refactor seconds!!!
}
