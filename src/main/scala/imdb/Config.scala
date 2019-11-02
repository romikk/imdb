package imdb

import scopt.OptionParser

import scala.concurrent.duration._

case class Config( minVotes: Int = 50,
                   topTitles: Int = 20,
                   sparkMaster: String = "local[*]",
                   timeout: Duration = 1 hour,
                   titleRatingsFile: String = "title.ratings.tsv.gz",
                   titleAkasFile: String = "title.akas.tsv.gz",
                   titlePrincipalsFile: String = "title.principals.tsv.gz",
                   nameBasicsFile: String = "name.basics.tsv.gz",
                 )
object Config {

  val parser: OptionParser[Config] = new scopt.OptionParser[Config]("imdb") {
    head("imdb", "1.x")

    opt[Int]('m', "minVotes") action { (x, c) =>
      c.copy(minVotes = x) } text("Minimum number of votes for a title (default: 50)")

    opt[Int]('t', "topTitles") action { (x, c) =>
      c.copy(topTitles = x) } text("Number of top titles to selec (default: 20)")

    opt[String]("sparkMaster") action { (x, c) =>
      c.copy(sparkMaster = x) } text("Spark Master host (default: local[*])")

    opt[String]("timeout") action { (x, c) =>
      c.copy(timeout = Duration.apply(x)) } text("Spark job timeout (default 1 hour)")

    opt[String]("titleRatingsFile") required() valueName("<file>") action { (x, c) =>
      c.copy(titleRatingsFile = x) } text("File containing the ratings information for titles (title.ratings.tsv.gz)")

    opt[String]("titleAkasFile") required() valueName("<file>") action { (x, c) =>
      c.copy(titleAkasFile = x) } text("File containing the title names (title.akas.tsv.gz)")

    opt[String]("titlePrincipalsFile") required() valueName("<file>") action { (x, c) =>
      c.copy(titlePrincipalsFile = x) } text("File containing title principals (title.principals.tsv.gz)")

    opt[String]("nameBasicsFile") required() valueName("<file>") action { (x, c) =>
      c.copy(nameBasicsFile = x) } text("File containing information for names (name.basics.tsv.gz )")

  }
}
