object QuickStartSparkSQL {

  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.{Column, DataFrame, SparkSession}

  def main ( args: Array[String] ): Unit = {
    val sc = SparkSession.builder ().appName ( "Quick start SparkSQL" )
      .master ( "local[*]" )
      .config ( "spark.hadoop.dfs.client.use.datanode.hostname", "true" )
      .getOrCreate

    sc.sparkContext.setLogLevel ( "ERROR" )

    val movies = loadDF ( sc, MoviesTest.moviesSource )

    countMovies ( movies )
    checkSchema ( movies )
    checkFirstFiveRows ( movies )
    movieWithLongestTitle ( movies )
    moviesWithYear ( movies )
    topThreeGenres ( movies )
    threeMoviesWithMostTags ( sc, movies )
    // Thread.sleep ( 20000000 ) for see on the spark UI
    sc.sparkContext.stop
  }

  private def threeMoviesWithMostTags ( sc: SparkSession, movies: DataFrame ): Unit = {
    val threeMoviesWithMostTags = loadDF ( sc, MoviesTest.tagsSource )
      .groupBy ( MoviesTest.movieId ).count.sort ( desc ( "count" ) ).limit ( 3 )
      .join ( movies, MoviesTest.movieId ).select ( MoviesTest.title, "count" )
    assert ( threeMoviesWithMostTags.collect.map ( v => (v ( 0 ), v ( 1 )).asInstanceOf [(String, Int)] ).toSet == MoviesTest.threeMoviesWithMostTags )
    threeMoviesWithMostTags.show
  }

  private def topThreeGenres ( movies: DataFrame ): Unit = {
    val genresCount = movies.select ( split ( movies.col ( MoviesTest.genres ), "\\|" ).as ( MoviesTest.genres ) )
      .select ( explode ( new Column ( MoviesTest.genres ) ).as ( "genre" ) ).groupBy ( "genre" )
      .count.sort ( desc ( "count" ) )
    assert ( genresCount.take ( 3 ).map ( v => (v ( 0 ), v ( 1 )) ) sameElements MoviesTest.topTreeGenres )
    genresCount.show ( 3 )
  }

  private def checkFirstFiveRows ( movies: DataFrame ): Unit = {
    assert ( movies.take ( 5 ).map ( _.mkString ( " " ) ) sameElements MoviesTest.firstFiveMovies )
    movies.show ( 5 )
  }

  private def checkSchema ( movies: DataFrame ): Unit = {
    assert ( movies.schema.map ( _.name ).toSet == MoviesTest.movieSchema )
    movies.printSchema
  }

  private def movieWithLongestTitle ( movies: DataFrame ): Unit = {
    val movieWithLongerTitle = movies.select ( movies.col ( MoviesTest.title ), length ( movies.col ( MoviesTest.title ) ).as ( "length" ) )
      .sort ( desc ( "length" ) ).head.toSeq
    assert ( (movieWithLongerTitle.head, movieWithLongerTitle ( 1 )) == MoviesTest.movieWithLongerTitle )
    println ( s"Movie with longer title ${MoviesTest.movieWithLongerTitle}" )
  }

  private def moviesWithYear ( movies: DataFrame ): Unit = {
    val moviesWithYears = movies.where ( movies.col ( MoviesTest.title ).rlike ( MoviesTest.yearRegex ) ).count
    assert ( moviesWithYears == MoviesTest.moviesWithYears )
    println ( s"#Movies with year: $moviesWithYears" )
  }

  private def countMovies ( movies: DataFrame ): Unit = {
    assert ( movies.count == MoviesTest.numberOfMovies )
    println ( s"There are: ${movies.count} movies" )
  }

  private def loadDF ( sparkSession: SparkSession, filePath: String ): DataFrame = sparkSession.read
    .options ( Map ( "header" -> "true", "inferSchema" -> "true" ) ) //other options like: mode, timeStampFormat, nullValue
    .csv ( filePath ) //or more generally use .format("csv").load(filePath)
}