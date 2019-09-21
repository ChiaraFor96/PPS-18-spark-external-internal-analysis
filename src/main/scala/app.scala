object QuickStartTest {
  case class Movie(movieId : Int ,title : String ,genres : Set[String])
  import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
  import org.apache.hadoop.conf.Configuration

  def main ( args: Array[String] ): Unit = {
    //TODO debit
    Configuration.addDefaultResource("configs/core-site.xml")
    Configuration.addDefaultResource("configs/hdfs-site.xml")
    val sc = SparkSession.builder().appName("Quick start")
      .master("local[*]")
      .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
      .getOrCreate().sqlContext
    import org.apache.spark.sql.SQLImplicits
    val ratings = loadDF(sc, "hdfs://hadoop:9000/ml-latest-small/ratings.csv")
    ratings.show()
   // val movies = sc.textFile("hdfs://localhost:9000/ml-latest-small/movies.csv")
    //val count = movies.count()
    //sqlContext.sparkSession.close()
  }
  def loadDF(sqlContext: SQLContext, filePath: String): DataFrame = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(filePath)
}