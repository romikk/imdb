package imdb

import imdb.Parsers._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.reflect.ClassTag
import scala.util.{Failure, Success}

case class Config( minVotes: Int = 50,
                   topTitles: Int = 20,
                   sparkMaster: String = "local[*]",
                   timeout: Duration = 1 hour,
                   titleRatingsFile: String = "title.ratings.tsv.gz",
                   titleAkasFile: String = "title.akas.tsv.gz",
                   titlePrincipalsFile: String = "title.principals.tsv.gz",
                   nameBasicsFile: String = "name.basics.tsv.gz",
                   )
object App {
  private val parser = new scopt.OptionParser[Config]("imdb") {
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

  def main(args : Array[String]) {
    parser.parse(args, Config()) map { config =>
      doRun(config)
    } getOrElse {
      println("Invalid command line arguments")
    }
  }

  private def doRun(config:Config) = {
    val conf = new SparkConf().setAppName("IMDB Ratings").setMaster(config.sparkMaster)
    val sc = new SparkContext(conf)

    // load title ratings and retain only those with rating >=50
    val ratings = readDataFile(sc, mapToTitleRating, config.titleRatingsFile)
      .filter(_.numVotes >= config.minVotes)
    // cache ratings because we will iterate it more than once
    ratings.cache()

    // load title and persons data
    val titleAkas = readDataFile(sc, mapToTitleAka, config.titleAkasFile)
    val titlePrincipals = readDataFile(sc, mapToTitlePrincipal, config.titlePrincipalsFile)
    val persons = readDataFile(sc, mapToPerson, config.nameBasicsFile)

    // get the average number of votes across all titles
    var (averageNumberOfVotes, total) = calculateAverage(ratings)

    // formula to calculate title ranks as per task
    val rankFormula = (x: TitleRating) => x.averageRating * x.numVotes / averageNumberOfVotes

    // build map of tconst->rank
    val topTitles: Map[String, Float] = takeTopTitles(ratings, config.topTitles, rankFormula)
      .map(x => (x._1.tconst, x._2))
      .toMap


    // find top title names
    val topTitleNames = findTopTitleNames(titleAkas, topTitles)
    topTitleNames.onComplete {
      case Success(names) => names.foreach(x => println(s"Rank: ${x._1}, Name: \"${x._2.get("GB").orElse(x._2.get("\\N")).getOrElse("")}\", other names: ${x._2.values.mkString(", ")}"))
      case Failure(e) => println(s"Failed to find top title names, exception = $e")
    }

    // find top title principals
    val topPrincipals = findTopPrincipals(titlePrincipals, persons, topTitles.keySet)
    topPrincipals.onComplete {
      case Success(principals) => principals.foreach(x => println(s"${x._1} was in ${x._2} top titles"))
      case Failure(e) => println(s"Failed to find top principals, exception = $e")
    }

    Await.result(topTitleNames, 1 hour)
    Await.result(topPrincipals, 1 hour)
  }

  /**
    * Find additional names to top titles sorted by title rank
    *
    * @param titleAkas
    * @param topTitles
    * @return
    */
  def findTopTitleNames(titleAkas: RDD[TitleAka], topTitles: Map[String,Float]) = {
    // filter title names using map of top titles
    titleAkas.filter(x => topTitles.contains(x.tconst))
      .groupBy(_.tconst) // group to get all names by tconst, then transform to tuple : (rank, Map[region, titleName])
      .map(x => (topTitles.getOrElse(x._1, 0f), x._2.map(x => (x.region, x.title)).toMap))
      .sortBy(_._1, ascending = false) // sort by rank, then print out english title name, and in all other langugages
      .collectAsync()
  }

  /**
    * Find all principals participating in given titles ranked by number of titles they appear
    * @param titlePrincipals
    * @param persons
    * @param titles
    * @return FutureAction[Seq[principal name, number of titles]]
    * */
  def findTopPrincipals(titlePrincipals: RDD[TitlePrincipal], persons: RDD[Person], titles: Set[String]) = {
    // find all principal persons for top titles
    val principals = titlePrincipals.filter(x => titles.contains(x.tconst))
      .map(_.nconst) // build a map nconst to number of times it appears in top titles
      .countByValue()

    // filter persons dataset by nconst found in previous step
    persons.filter(x => principals.contains(x.nconst))
      .map(x => (x, principals.get(x.nconst)))
      .sortBy(_._2, ascending = false) // sort and print out persons names
      .map(x => (x._1.primaryName, x._2.getOrElse(0)))
      .collectAsync()
  }

  /**
    * Calculate average of numVotes across TitleRating dataset
    * @param ratings
    * @return (average, total)
    */
  def calculateAverage(ratings: RDD[TitleRating]): (Float, Int) = {
    ratings.map((x: TitleRating) => (x.numVotes.toFloat, 1))
           .reduce((a: (Float, Int), b: (Float, Int)) =>
                        ((a._1 * a._2) / (a._2 + b._2) + (b._1 * b._2) / (a._2 + b._2), a._2 + b._2))
  }

  def takeTopTitles(ratings: RDD[TitleRating], num:Int, rankFormula: TitleRating => Float): Seq[(TitleRating, Float)] = {
    ratings.map((x: TitleRating) => (x, rankFormula(x)))
           .sortBy(_._2, ascending = false).cache()
           .take(num)
  }

  def readDataFile[T:ClassTag](sc: SparkContext, parser: String => T, filePath:String): RDD[T] = {
    var ratingsData = sc.textFile(filePath)
    // Extract the first row which is the header
    val header = ratingsData.first()

    // Filter out the header from the dataset
    ratingsData.filter(row => row != header)
               .map[T](parser)
  }

}
