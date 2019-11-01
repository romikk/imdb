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
  }
}
