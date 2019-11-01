package imdb

import imdb.Parsers._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.reflect.ClassTag
import scala.util.{Failure, Success}

object App {


  def main(args : Array[String]) {

    val conf = new SparkConf().setAppName("IMDB Ratings").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // load title ratings and retain only those with rating >=50
    val ratings = readDataFile(sc, mapToTitleRating, "C:\\dev\\imdb\\title.ratings.tsv.gz")
                       .filter(_.numVotes >= 50)
    // cache ratings because we will iterate it more than once
    ratings.cache()

    // load title and persons data
    val titleAkas = readDataFile(sc, mapToTitleAka, "C:\\dev\\imdb\\title.akas.tsv.gz")
    val titlePrincipals = readDataFile(sc, mapToTitlePrincipal, "C:\\dev\\imdb\\title.principals.tsv.gz")
    val persons = readDataFile(sc, mapToPerson, "C:\\dev\\imdb\\name.basics.tsv.gz")

    // get the average number of votes across all titles
    var (averageNumberOfVotes, total) = calculateAverage(ratings)

    // formula to calculate title ranks as per task
    val rankFormula = (x: TitleRating) => x.averageRating * x.numVotes/averageNumberOfVotes

    // build map of tconst->rank
    val topTitles : Map[String,Float] = takeTopTitles(ratings, 20, rankFormula)
                                        .map(x => (x._1.tconst, x._2))
                                        .toMap

    val topTitleNames = findTopTitleNames(titleAkas, topTitles)
    topTitleNames.onComplete {
      case Success(names) => names.foreach(x => println(s"Rank: ${x._1}, Name: ${x._2.get("GB").orElse(x._2.get("\\N")).getOrElse("")}, other names: ${x._2.values.mkString(", ")}"))
      case Failure(e) => println(s"Failed to find top title names, exception = $e")
    }

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
