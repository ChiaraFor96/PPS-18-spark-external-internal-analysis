/*
launch notes:
- add '127.0.0.1   localhost   hadoop' in etc/hosts
- docker build -t container-name . (from docker/ folder)
- docker run -ti --hostname hadoop -p 50070:50070 -p 9000:9000 -p 50075:50075 -p 50010:50010 container-name
*/
object QuickStartTest {
  case class Movie(movieId : Int ,title : String ,genres : Set[String])
  import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

  def main ( args: Array[String] ): Unit = {
    import org.apache.log4j.Logger
    val log = Logger.getLogger(getClass.getName)
    val sc = SparkSession.builder().appName("Quick start")
      .master("local[*]")
      .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
      .getOrCreate().sqlContext

    val movies = loadDF(sc, "hdfs://hadoop:9000/ml-latest-small/movies.csv")
    log.info(s"There are: ${movies.count()} movies")

    sc.sparkSession.close()
  }
  def loadDF(sqlContext: SQLContext, filePath: String): DataFrame = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(filePath)
}