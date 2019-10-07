package structuredStreaming

object SparkStructuredStreaming {
  def main ( args: Array[String] ): Unit = {
    import DatasetManipulationUtils._
    import org.apache.spark.sql.SparkSession
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.streaming.Trigger
    //val log = org.apache.log4j.Logger.getLogger ( getClass.getName )

    val spark = SparkSession.builder.appName ( "Structured Streaming" )
      .master ( "local[*]" )
      .getOrCreate
    spark.sparkContext.setLogLevel ( "ERROR" )

    val dataset1 = getDataset ( spark, 10 )
    val dataset2 = getDataset ( spark, 20 )
    val dataset3 = getDataset ( spark, 30 )

    //udf function
    dataset1.select ( udf { s: StreamValueType => s.hashCode }.apply ( valueColumn.column ).as ( "hash code" ) )
      .writeStream
      //.trigger ( Trigger.Continuous ( "5 seconds" ) ) //use experimental CP (in Spark 2.3+).. don't work :)
      .format ( "console" )
      .start

    //join 3 Datasets
    val datasets = dataset1.join ( dataset2, timestampColumn.value )
      .join ( dataset3, timestampColumn.value )
      .withColumn ( avgValueColumn.value,
        (dataset1.col ( valueColumn.value ) + dataset2.col ( valueColumn.value ) + dataset3.col ( valueColumn.value )) / 3 )
      //TODO max lenght value in 3 cols
    datasets.drop ( datasets.columns.filter ( _.equals ( valueColumn.value ) ): _* )
      //.drop ( datasets.columns.filter ( _.equals ( lengthColumn.value ) ): _* )
      //.withColumnRenamed(avgValueColumn.value, valueColumn.value)
      .withWatermark ( timestampColumn.value, "5 seconds")
      .groupBy (
        window ( timestampColumn.column, "10 seconds", "5 seconds" ),
        dataset1.col(lengthColumn.value))
      .count
      .writeStream
      .format ( "console" )
      /*.sort ( avgValueColumn.column )
      .outputMode ( "complete" ) not here!*/
      .trigger ( Trigger.ProcessingTime ( "0 seconds" ) ) //fixed trigger, CP not supported in JOIN
      .start

    //await any computation
    spark.streams.awaitAnyTermination
  }

  //TODO refactor seconds!!!
}
