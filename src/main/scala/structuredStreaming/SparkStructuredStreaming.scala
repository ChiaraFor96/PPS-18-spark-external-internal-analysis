package structuredStreaming

import structuredStreaming.StreamingDataOrganizerUtils.structuredManipulationUtilities.StreamRecord

object SparkStructuredStreaming {
  def main ( args: Array[String] ): Unit = {
    import StreamingDataOrganizerUtils._
    import StreamingDataOrganizerUtils.structuredManipulationUtilities.ColumnOperations._
    import org.apache.spark.sql.Dataset
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.streaming.Trigger

    val datasets: Seq[Dataset[StreamRecord]] = Seq ( getRateDataset ( 10 ),
      getRateDataset ( 20 ),
      getRateDataset ( 30 ) )
    //docker inspect --format "{{ .NetworkSettings.IPAddress }}" $(docker ps -q)
    //getSocketDataFrame("172.17.0.3", 9999).select("*").writeStream.format("console").start

    //udf function
    datasets.head.select ( udf { s: Long => s.hashCode }.apply ( idColumn ).as ( "hash code" ) )
      .writeStream
      .trigger ( Trigger.ProcessingTime ( "2 seconds" ) )
      //.trigger ( Trigger.Continuous ( "5 seconds" ) ) //use experimental CP (in Spark 2.3+).. don't work :)
      .format ( "console" )
      .start

    val computedDatasets = datasets.map ( _.toDF ).reduce ( ( a, b ) => a.join ( b, idColumn.name ) )
      .withColumn ( avgValueColumn.name, avgOfColumns ( datasets.map ( _.col ( valueColumn.name ) ) ) )
      .withColumn ( avgTimestampColumn.name,
        to_timestamp ( avgOfColumns ( datasets.map ( ds => unix_timestamp ( ds.col ( timestampColumn.name ) ) ) ) ) )

    computedDatasets.drop ( computedDatasets.columns.filter ( _.equals ( valueColumn.name ) ): _* )
      .drop ( computedDatasets.columns.filter ( _.equals ( timestampColumn.name ) ): _* )
      .withWatermark ( avgTimestampColumn.name, "5 seconds" )
      .groupBy ( window ( avgTimestampColumn, "10 seconds", "5 seconds" ) )
      .mean ( avgValueColumn.name )
      .writeStream
      .format ( "console" )
      .start

    spark.streams.awaitAnyTermination
  }

  //TODO refactor seconds!!!
}
