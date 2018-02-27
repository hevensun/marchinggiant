import scala.io.Source
import scala.util.Try

/**
  * Created by cailiming on 18-2-9.
  */
object Cosine {
    def main(args: Array[String]): Unit = {
        val ans = Source.fromFile("/home/mi/Desktop/debug")
            .getLines()
            .map { line =>
                line.substring(17).substring(0, 6).toDouble
            }
            .toSeq

        println(ans.count(a => a > 0.0 && a <= 0.02))
        println(ans.count(a => a > 0.02 && a <= 0.05))
        println(ans.count(a => a > 0.05 && a <= 0.1))
        println(ans.count(a => a > 0.1 && a <= 0.15))
        println(ans.count(a => a > 0.15 && a <= 0.2))
        println(ans.count(a => a > 0.2 && a <= 0.3))
        println(ans.count(a => a > 0.3 && a <= 0.4))
        println(ans.count(a => a > 0.4 && a <= 0.5))
        println(ans.count(a => a > 0.5 && a <= 0.6))
        println(ans.count(a => a > 0.6 && a <= 0.7))
        println(ans.count(a => a > 0.7 && a <= 0.8))
        println(ans.count(a => a > 0.8 && a <= 0.9))
        println(ans.count(a => a > 0.9 && a <= 1))
        println(ans.count(a => a > 1 && a <= 1.5))
        println(ans.count(a => a > 1.5))
    }


    def main1(args: Array[String]): Unit = {
        val c1 = Seq(
            "1" -> 1.0,
            "2" -> 2.0,
            "3" -> 3.0
        )

        val c2 = Seq(
            "1" -> 0.1,
            "2" -> 0.8,
            "3" -> 3.0
        )

        println(calCos(c1, c2))
        println(calCoc2(c1, c2))
    }

    def calCos(category1: Seq[(String, Double)], category2: Seq[(String, Double)]) = {
        val normalized1 = normalize(category1)
        val normalized2 = normalize(category2)

        val cate1Map = normalized1.toMap
        normalized2.map(c => Try(cate1Map(c._1) * c._2).getOrElse(0.0)).sum
    }

    def normalize(category: Seq[(String, Double)]) = {
        val num = Math.sqrt(category.map(c => Math.pow(c._2, 2)).sum)
        category.map(c => c._1 -> c._2 / num)
    }

    def calCoc2(category1: Seq[(String, Double)], category2: Seq[(String, Double)]) = {
        val num1 = Math.sqrt(category1.map(c => Math.pow(c._2, 2)).sum)
        val num2 = Math.sqrt(category2.map(c => Math.pow(c._2, 2)).sum)

        val cate1Map = category1.toMap
        val tmpSum = category2.map(c => Try(cate1Map(c._1) * c._2).getOrElse(0.0)).sum

        tmpSum / (num1 * num2)
    }
}
