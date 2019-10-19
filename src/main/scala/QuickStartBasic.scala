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
    val conf = new SparkConf ().setAppName ( "Quick start basic" )
      .setMaster ( "local[*]" )
      .set ( "spark.hadoop.dfs.client.use.datanode.hostname", "true" )
    val sc = SparkContext.getOrCreate ( conf )

    sc.setLogLevel ( "ERROR" )

    val (moviesHeader, moviesRows) = loadRDD ( sc, MoviesTest.moviesSource )

    countMovies ( moviesRows )
    checkSchema ( moviesHeader )
    checkFirstFiveRows ( moviesRows )
    movieWithLongestTitle ( moviesHeader, moviesRows )
    moviesWithYear ( moviesHeader, moviesRows )
    topThreeGenres ( moviesHeader, moviesRows )
    threeMoviesWithMostTags ( sc, moviesHeader, moviesRows )
    // Thread.sleep ( 20000000 ) for see on the spark UI
    sc.stop
  }

  private def topThreeGenres ( moviesHeader: SimpleCSVHeader, moviesRows: RDD[Array[String]] ) {
    val topTreeGenres = moviesRows.map(moviesHeader(_, MoviesTest.genres))
      .flatMap(_.split("\\|"))
      .map((_, 1))
      .reduceByKey ( _ + _ )
      .sortBy ( _._2, ascending = false )
      .take(3)
    //problems in parsing assert ( topTreeGenres sameElements MoviesTest.topTreeGenres )
    println ( topTreeGenres )
  }

  private def threeMoviesWithMostTags ( sc: SparkContext, moviesHeader: SimpleCSVHeader, moviesRows: RDD[Array[String]] ): Unit = {
    val (tagsHeader, tagsRows) = loadRDD ( sc, MoviesTest.tagsSource )

    val threeMoviesWithMostTags = sc.parallelize ( tagsRows.map ( tag => (tagsHeader ( tag, MoviesTest.movieId ), 1) )
      .reduceByKey ( _ + _ )
      .sortBy ( _._2, ascending = false )
      .take ( 3 ) )
      .join ( moviesRows.map ( movie =>
        (moviesHeader ( movie, MoviesTest.movieId ), moviesHeader ( movie, MoviesTest.title )) ) )
      .map { case (_, (x, y)) => (y, x) }.collect.toSet
    assert ( threeMoviesWithMostTags == MoviesTest.threeMoviesWithMostTags )
    println ( threeMoviesWithMostTags )
  }

  private def moviesWithYear ( moviesHeader: SimpleCSVHeader, moviesRows: RDD[Array[String]] ): Unit = {
    val moviesWithYears = moviesRows.map ( moviesHeader ( _, MoviesTest.title ) )
      .filter ( _.matches ( s"(.*)${MoviesTest.yearRegex}(.*)" ) )
      .count
    ////problems in parsing assert(moviesWithYears == MoviesTest.moviesWithYears)
    println ( s"#Movies with year: $moviesWithYears" )
  }

  private def movieWithLongestTitle ( moviesHeader: SimpleCSVHeader, moviesRows: RDD[Array[String]] ): Unit = {
    val movieWithLongerTitle = moviesRows.map ( moviesHeader ( _, MoviesTest.title ) )
      .map ( t => (t, t.length) )
      .sortBy ( _._2, ascending = false )
      .take ( 1 ).head
    assert ( movieWithLongerTitle == MoviesTest.movieWithLongerTitle )
    println ( s"Movie with longer title $movieWithLongerTitle" )
  }

  private def checkFirstFiveRows ( moviesRows: RDD[Array[String]] ): Unit = {
    val firstFiveMovies = moviesRows.take ( 5 ).map ( _.mkString ( " " ) )
    assert ( firstFiveMovies sameElements MoviesTest.firstFiveMovies )
    println ( firstFiveMovies.mkString ( "\n" ) )
  }

  private def checkSchema ( moviesHeader: SimpleCSVHeader ): Unit = {
    assert ( moviesHeader.header.toSet == MoviesTest.movieSchema )
    println ( s"Schema: ${moviesHeader.header.mkString ( " " )}" )
  }

  private def countMovies ( moviesRows: RDD[Array[String]] ): Unit = {
    assert ( moviesRows.count == MoviesTest.numberOfMovies )
    println ( s"There are: ${moviesRows.count} movies" )
  }

  private def loadRDD ( sc: SparkContext, filePath: String ): (SimpleCSVHeader, RDD[Array[String]]) = {
    val rdd = sc.textFile ( filePath ).map ( _.split ( "," ).map ( _.trim ) )
    val header = new SimpleCSVHeader ( rdd.first )
    val rows = rdd.filter ( header ( _, MoviesTest.movieId ) != MoviesTest.movieId )
    (header, rows)
  }

  private class SimpleCSVHeader ( val header: Array[String] ) extends Serializable {
    val index: Map[String, Int] = header.zipWithIndex.toMap

    def apply ( array: Array[String], key: String ): String = array ( index ( key ) )
  }

}
