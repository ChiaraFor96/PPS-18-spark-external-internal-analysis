/*
launch notes:
- add '127.0.0.1   localhost   hadoop' in etc/hosts
- docker build -t container-name . (from docker/ folder)
- docker run -ti --hostname hadoop -p 50070:50070 -p 9000:9000 -p 50075:50075 -p 50010:50010 container-name
*/
object QuickStartSparkSQL {

  import TestValues._
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.{Column, DataFrame, SQLContext, SparkSession}

  def main ( args: Array[String] ): Unit = {
    val sc = SparkSession.builder ().appName ( "Quick start SparkSQL" )
      .master ( "local[*]" )
      .config ( "spark.hadoop.dfs.client.use.datanode.hostname", "true" )
      .getOrCreate ().sqlContext

    sc.sparkContext.setLogLevel ( "ERROR" )

    val movies = loadDF ( sc, moviesSource )

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

  private def threeMoviesWithMostTags ( sc: SQLContext, movies: DataFrame ): Unit = {
    val moviesTags = loadDF ( sc, tagsSource )
      .groupBy ( movieId ).count.sort ( desc ( "count" ) ).limit ( 3 )
      .join ( movies, movieId ).select ( title, "count" )
    assert ( moviesTags.collect.map ( v => (v ( 0 ), v ( 1 )).asInstanceOf [(String, Int)] ).toSet == threeMoviesWithMostTags )
    moviesTags.show
  }

  private def topThreeGenres ( movies: DataFrame ): Unit = {
    val genresCount = movies.select ( split ( movies.col ( genres ), "\\|" ).as ( genres ) )
      .select ( explode ( new Column ( genres ) ).as ( "genre" ) ).groupBy ( "genre" )
      .count
    assert ( genresCount.take ( 3 ).map ( v => (v ( 0 ), v ( 1 )) ) sameElements topTreeGenres )
    genresCount.show ( 3 )
  }

  private def checkFirstFiveRows ( movies: DataFrame ): Unit = {
    assert ( movies.take ( 5 ).map ( _.mkString ( " " ) ) sameElements firstFiveMovies )
    movies.show ( 5 )
  }

  private def checkSchema ( movies: DataFrame ): Unit = {
    assert ( movies.schema.map ( _.name ).toSet == movieSchema )
    movies.printSchema
  }

  private def movieWithLongestTitle ( movies: DataFrame ): Unit = {
    val longerTitle = movies.select ( movies.col ( title ), length ( movies.col ( title ) ).as ( "length" ) )
      .sort ( desc ( "length" ) ).head.toSeq
    assert ( (longerTitle.head, longerTitle ( 1 )) == movieWithLongerTitle )
    println ( s"Movie with longer title $movieWithLongerTitle" )
  }

  private def moviesWithYear ( movies: DataFrame ): Unit = {
    val moviesYears = movies.where ( movies.col ( title ).rlike ( yearRegex ) ).count
    assert ( moviesYears == TestValues.moviesWithYears )
    println ( s"#Movies with year: $moviesYears" )
  }

  private def countMovies ( movies: DataFrame ): Unit = {
    assert ( movies.count == numberOfMovies )
    println ( s"There are: ${movies.count} movies" )
  }

  private def loadDF ( sqlContext: SQLContext, filePath: String ): DataFrame = sqlContext.read
    .options ( Map ( "header" -> "true", "inferSchema" -> "true" ) ) //other options like: mode, timeStampFormat, nullValue
    .csv ( filePath ) //or more generally use .format("csv").load(filePath)
}