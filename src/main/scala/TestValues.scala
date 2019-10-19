object TestValues {
  val numberOfMovies: Int = 9742
  val movieId: String = "movieId"
  val title: String = "title"
  val genres: String = "genres"
  val movieSchema: Set[String] = Set ( movieId, title, genres )
  private val source = "hdfs://hadoop:9000/ml-latest-small"
  val tagsSource: String = s"${source}/tags.csv"
  val moviesSource: String = s"${source}/movies.csv"
  val yearRegex: String = """[0-9]{4}"""
  val moviesWithYears: Int = 9730
  val movieWithLongerTitle: (String, Int) = ("Dragon Ball Z the Movie: The World's Strongest (a.k.a. Dragon Ball Z: The Strongest Guy in The World) (Doragon bôru Z: Kono yo de ichiban tsuyoi yatsu) (1990)", 158)
  val firstFiveMovies: Array[String] = Array ( Movie ( 1, "Toy Story (1995)", Seq ( "Adventure", "Animation", "Children", "Comedy", "Fantasy" ) ),
    Movie ( 2, "Jumanji (1995)", Seq ( "Adventure", "Children", "Fantasy" ) ),
    Movie ( 3, "Grumpier Old Men (1995)", Seq ( "Comedy", "Romance" ) ),
    Movie ( 4, "Waiting to Exhale (1995)", Seq ( "Comedy", "Drama", "Romance" ) ),
    Movie ( 5, "Father of the Bride Part II (1995)", Seq ( "Comedy" ) ) ).map(_.toString)

  val topTreeGenres : Seq[(String, Int)] = Seq(("Crime", 1199), ("Romance", 1596), ("Thriller", 1894))
  val threeMoviesWithMostTags : Set[(String, Int)] = Set(("Pulp Fiction (1994)", 181), ("Fight Club (1999)", 54), ("2001: A Space Odyssey (1968)", 41))

  case class Movie ( movieId: Int, title: String, genres: Seq[String] ) {
    override def toString: String = s"$movieId $title ${genres.mkString("|")}"
  }

}
