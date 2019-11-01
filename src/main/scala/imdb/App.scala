package imdb

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.ClassTag

case class TitleRating(tconst: String, averageRating: Float, numVotes: Int)

object App {


  def main(args : Array[String]) {

    val conf = new SparkConf().setAppName("IMDB Ratings").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // load title ratings and retain only those with rating >=50
    val ratings = readDataFile(sc, mapToTitleRating, "C:\\dev\\imdb\\title.ratings.tsv.gz")
                       .filter(_.numVotes >= 50)
    // cache ratings because we will iterate it more than once
    ratings.cache()

    // get the average number of votes across all titles
    var (averageNumberOfVotes, total) = calculateAverage(ratings)

    println(s"averageNumberOfVotes: $averageNumberOfVotes")

    val scoreFormula = (x: TitleRating) => x.averageRating * x.numVotes/averageNumberOfVotes

    val topTitles : Map[String,Float] = takeTopTitles(ratings, 20, scoreFormula)
                                        .map(x => (x._1.tconst, x._2))
                                        .toMap

    topTitles.foreach(println)

  }

  def mapToTitleRating(line: String): TitleRating = {
    val fields = line.split('\t')
    TitleRating(fields(0), fields(1).toFloat, fields(2).toInt)
  }

  
  def calculateAverage(ratings: RDD[TitleRating]): (Float, Int) = {
    ratings.map((x: TitleRating) => (x.numVotes.toFloat, 1))
           .reduce((a: (Float, Int), b: (Float, Int)) =>
                        ((a._1 * a._2) / (a._2 + b._2) + (b._1 * b._2) / (a._2 + b._2), a._2 + b._2))
  }

  def takeTopTitles(ratings: RDD[TitleRating], num:Int, scoreFormula: TitleRating => Float): Seq[(TitleRating, Float)] = {
    ratings.map((x: TitleRating) => (x, scoreFormula(x)))
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
