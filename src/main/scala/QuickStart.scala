/*
launch notes:
- add '127.0.0.1   localhost   hadoop' in etc/hosts
- docker build -t container-name . (from docker/ folder)
- docker run -ti --hostname hadoop -p 50070:50070 -p 9000:9000 -p 50075:50075 -p 50010:50010 container-name
*/
object QuickStart {
  import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession, Column}
  import org.apache.spark.sql.functions._

  def main ( args: Array[String] ): Unit = {
    import org.apache.log4j.Logger
    val log = Logger.getLogger(getClass.getName)
    val sc = SparkSession.builder().appName("Quick start")
      .master("local[*]")
      .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
      .getOrCreate().sqlContext

    val movies = loadDF(sc, "hdfs://hadoop:9000/ml-latest-small/movies.csv")
    log.info(s"There are: ${movies.count()} movies")
    movies.printSchema
    movies.show(5)
    log.info(s"Movie with longer title ${
      movies.select(movies.col("title"), length(movies.col("title")).as("length"))
        .sort(desc("length")).head}")
    log.info(s"#Movies with year: ${movies.select(movies.col("title").rlike("[0-9]{4}")).count}")

    movies.select(split(movies.col("genres"), "\\|").as("genres"))
        .select(explode(new Column("genres")).as("genre")).groupBy("genre")
        .count.show(3)

    //load tags
    loadDF(sc, "hdfs://hadoop:9000/ml-latest-small/tags.csv")
      .groupBy("movieId").count.sort(desc("count")).limit(3)
      .join(movies, "movieId").select("title", "count").show()

    Thread.sleep(20000000) //for see on the spark UI
    sc.sparkSession.close()
  }
  def loadDF(sqlContext: SQLContext, filePath: String): DataFrame = sqlContext.read
    .options(Map("header" -> "true", "inferSchema" -> "true")) //other options like: mode, timeStampFormat, nullValue
    .csv(filePath)
}