/*
launch notes:
- add '127.0.0.1   localhost stream' in etc/hosts
- docker build -t container-name . (from docker/stream-container folder)
- docker run -ti --hostname stream -p 50070:50070 -p 9999:9999 -p 50075:50075 -p 50010:50010 container-name
*/
package streaming.structuredStreaming

object SparkStructuredStreaming {
  def main ( args: Array[String] ): Unit = {
    import StreamingDataOrganizerUtils._
    import StreamingDataOrganizerUtils.structuredManipulationUtilities.ColumnOperations._
    import streaming.structuredStreaming.StreamingDataOrganizerUtils.structuredManipulationUtilities.StreamRecord
    import org.apache.spark.sql.Dataset
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.streaming.Trigger
    import org.apache.spark.sql.types.{StructType, DataTypes}
    import streaming.structuredStreaming.StreamingDataOrganizerUtils.CalendarInterval._
    import spark.implicits._

    val datasets: Seq[Dataset[StreamRecord]] = Seq ( getRateDataset ( 10 ),
      getRateDataset ( 20 ),
      getRateDataset ( 30 ) )

    val externalSource = spark.readStream
      .schema(new StructType().add("id",  DataTypes.StringType)
        .add("timestamp", DataTypes.StringType)
        .add("value",  DataTypes.StringType))
      .option ( "header", "true" )
      .format("csv")
      .load("hdfs://stream:9999/*.csv")
      .withColumn(valueColumn.name, valueColumn.cast("double"))
      .withColumn(idColumn.name, idColumn.cast("int"))
      .withColumn(timestampColumn.name, timestampColumn.cast("timestamp"))
      .as[StreamRecord]

    externalSource.select("*").writeStream.format ( "console" ).start
    //udf function
    datasets.head.select ( udf { s: Long => s.hashCode }.apply ( idColumn ).as ( "hash code" ) )
      .writeStream
      .trigger ( Trigger.ProcessingTime (  Seconds(2).toString ) )
      //.trigger ( Trigger.Continuous ( Seconds(5).toString ) ) //use experimental CP (in Spark 2.3+).. don't work :)
      .format ( "console" )
      .start

    var computedDatasets = datasets.map ( _.toDF ).reduce ( ( a, b ) => a.join ( b, idColumn.name ) )
      .withColumn ( avgValueColumn.name, avgOfColumns ( datasets.map ( _.col ( valueColumn.name ) ) ) )
      .withColumn ( avgTimestampColumn.name,
        to_timestamp ( avgOfColumns ( datasets.map ( ds => unix_timestamp ( ds.col ( timestampColumn.name ) ) ) ) ) )

    computedDatasets = computedDatasets.drop ( computedDatasets.columns.filter ( _.equals ( valueColumn.name ) ): _* )
      .drop ( computedDatasets.columns.filter ( _.equals ( timestampColumn.name ) ): _* )


   // computedDatasets.join(externalSource, idColumn.name).select("*").writeStream.format("console").start

    computedDatasets.join(externalSource, idColumn.name)
      .withWatermark ( avgTimestampColumn.name, Seconds(5).toString )
      .groupBy ( window ( avgTimestampColumn, Minutes(1).toString, Seconds(30).toString ) )
      .agg ( mean(avgValueColumn.name).as("InternalAVG"), mean( valueColumn.name ).as("ExternalAVG") )

      .writeStream
      .format ( "console" )
      .start

    spark.streams.awaitAnyTermination
  }
}
