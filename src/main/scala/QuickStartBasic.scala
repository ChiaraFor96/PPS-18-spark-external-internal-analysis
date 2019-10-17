/*
launch notes:
- add '127.0.0.1   localhost   hadoop' in etc/hosts
- docker build -t container-name . (from docker/ folder)
- docker run -ti --hostname hadoop -p 50070:50070 -p 9000:9000 -p 50075:50075 -p 50010:50010 container-name
*/
object QuickStartBasic {
  import org.apache.spark.rdd.RDD
  import org.apache.spark.{SparkConf, SparkContext}

  def main ( args: Array[String] ): Unit = {
    val conf = new SparkConf().setAppName("Quick start basic")
      .setMaster("local[*]")
      .set("spark.hadoop.dfs.client.use.datanode.hostname", "true" )
    val sc = SparkContext.getOrCreate(conf)

    sc.setLogLevel("ERROR")

    val movies = loadRDD ( sc, "hdfs://hadoop:9000/ml-latest-small/movies.csv" )
    val moviesHeader = new SimpleCSVHeader(movies.first)
    val moviesRows = movies.filter(moviesHeader(_,"movieId") != "movieId") // filter the header out

    println(s"Schema: ${movies.first.mkString(" ")}")
    println(s"There are: ${moviesRows.count} movies")
    println(moviesRows.take(5).map(_.mkString(" ")).mkString("\n"))

    println(s"Movie with longer title ${moviesRows.map(moviesHeader(_, "title"))
      .map(t => (t, t.length))
      .sortBy(_._2, ascending = false)
      .take(1).mkString(" ")}")

    println(s"#Movies with year: ${moviesRows.map(moviesHeader(_, "title"))
        .filter("""[0-9]{4}""".r.findFirstIn(_).isDefined)
        .count
    }")

    /* TODO
    movies.select ( split ( movies.col ( "genres" ), "\\|" ).as ( "genres" ) )
      .select ( explode ( new Column ( "genres" ) ).as ( "genre" ) ).groupBy ( "genre" )
      .count.show ( 3 )*/

    //load tags
    val tags = loadRDD ( sc, "hdfs://hadoop:9000/ml-latest-small/tags.csv" )
    val tagsHeader = new SimpleCSVHeader(tags.first)
    val tagsRows = tags.filter(tagsHeader(_,"movieId") != "movieId") // filter the header out

    /* TODO
    tags
      .groupBy ( "movieId" ).count.sort ( desc ( "count" ) ).limit ( 3 )
      .join ( movies, "movieId" ).select ( "title", "count" ).show ()*/

    // Thread.sleep ( 20000000 ) for see on the spark UI*/
    sc.stop
  }

  def loadRDD ( sc: SparkContext, filePath: String ): RDD[Array[String]] = sc.textFile(filePath).map(_.split(",").map(_.trim))

  class SimpleCSVHeader(header:Array[String]) extends Serializable {
    val index: Map[String, Int] = header.zipWithIndex.toMap
    def apply(array:Array[String], key:String):String = array(index(key))
  }
}
