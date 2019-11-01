package imdb

case class TitleRating(tconst: String, averageRating: Float, numVotes: Int)
case class TitleAka(tconst: String, title: String, region: String)
case class TitlePrincipal(tconst: String, nconst: String)
case class Person(nconst: String, primaryName: String)

object Parsers {

  def mapToTitleRating(line: String): TitleRating = {
    val fields = line.split('\t')
    TitleRating(fields(0), fields(1).toFloat, fields(2).toInt)
  }

  def mapToTitleAka(line: String): TitleAka = {
    val fields = line.split('\t')
    TitleAka(fields(0), fields(2), fields(3))
  }

  def mapToTitlePrincipal(line: String): TitlePrincipal = {
    val fields = line.split('\t')
    TitlePrincipal(fields(0), fields(2))
  }

  def mapToPerson(line: String): Person = {
    val fields = line.split('\t')
    Person(fields(0), fields(1))
  }
}
