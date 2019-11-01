package imdb

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{Matchers, WordSpec}


class MySpec extends WordSpec with Matchers with SharedSparkContext {
  "My analytics" should {
    "calculate average" in {
      val data = Seq(TitleRating("aaa", 123, 100),
                     TitleRating("bbb", 123, 200),
                     TitleRating("ccc", 123, 300))
      val (average, total) = App.calculateAverage(sc.makeRDD(data))
      total shouldBe data.length
      average shouldBe 600/data.length
    }

    "find top titles" in {
      val data = Seq(TitleRating("aaa", 1.23f, 100),
                     TitleRating("bbb", 2.23f, 200),
                     TitleRating("ccc", 3.23f, 300))
      val topTitles = App.takeTopTitles(sc.makeRDD(data), 2, _.averageRating)
      topTitles should have size 2
      topTitles.head._1 shouldBe TitleRating("ccc", 3.23f, 300)
    }
  }
}
