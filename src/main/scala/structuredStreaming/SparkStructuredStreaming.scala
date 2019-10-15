/*
launch notes:
- add '127.0.0.1   localhost stream' in etc/hosts
- docker build -t container-name . (from docker/strem folder)
- docker run -ti --hostname stream -p 50070:50070 -p 9999:9999 -p 50075:50075 -p 50010:50010 container-name
*/
package structuredStreaming

import structuredStreaming.StreamingDataOrganizerUtils.structuredManipulationUtilities.StreamRecord

object SparkStructuredStreaming {
  def main ( args: Array[String] ): Unit = {
    import StreamingDataOrganizerUtils._
    import StreamingDataOrganizerUtils.structuredManipulationUtilities.ColumnOperations._
    import org.apache.spark.sql.Dataset
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.streaming.Trigger
    import org.apache.spark.sql.types.{StructType, DataTypes}

    val datasets: Seq[Dataset[StreamRecord]] = Seq ( getRateDataset ( 10 ),
      getRateDataset ( 20 ),
      getRateDataset ( 30 ) );
    //docker inspect --format "{{ .NetworkSettings.IPAddress }}" $(docker ps -q)
    //docker run -ti --hostname nethost -p 9999:9999 netcat (nethost in etc/hosts)
    val df = spark.readStream
      .schema(new StructType().add("date", DataTypes.StringType).add("value", DataTypes.StringType))
      .options ( Map ( "header" -> "true", "inferSchema" -> "true" ) )
      .format("csv")
      .load("hdfs://stream:9999/*.csv")
    //TODO check what's the problem of data.csv change

      df.select("*")
        .writeStream
        .trigger ( Trigger.ProcessingTime ( "1 seconds" ) )
        .format("console")
        .start

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
