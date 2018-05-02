package com.xiaomi.ad.keyword.marchinggiant.browsernews

import com.twitter.scalding.Args
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.xiaomi.data.commons.spark.HdfsIO._
import com.xiaomi.data.spec.log.profile.ArticleInfo

import scala.collection.JavaConversions

/**
  * Created by cailiming on 17-8-25.
  */
object CharacterWords {
    def main(args: Array[String]): Unit = {
        val argv = Args(args)
        execute(argv, new SparkConf())
    }

    def execute(args: Args, sparkConf: SparkConf) = {
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._

        val categories = Seq(
            "汽车" -> "auto",
            "科技" -> "science",
            "教育" -> "edu",
            "家居" -> "home",
            "房产" -> "house",
            "健康" -> "health",
            "育儿" -> "parenting",
            "旅行" -> "traveling"
        )

        val inputRdd = spark.sparkContext.thriftSequenceFile(args("input"), classOf[ArticleInfo])

        categories
            .foreach{ case (specialCategory, engPath) =>
                val curWords = inputRdd
                    .filter { a =>
                        val keywords = if(a.keywords == null) Seq() else JavaConversions.asScalaBuffer(a.keywords)
                        val extKeywords = if(a.extKeywords == null) Seq() else JavaConversions.asScalaBuffer(a.extKeywords)
                        val allKeywords = keywords ++ extKeywords
                        a != null && a.category != null && a.category.contains(specialCategory) && allKeywords.nonEmpty
                    }
                    .flatMap{ a =>
                        val keywords = if(a.keywords == null) Seq() else JavaConversions.asScalaBuffer(a.keywords)
                        val extKeywords = if(a.extKeywords == null) Seq() else JavaConversions.asScalaBuffer(a.extKeywords)
                        val allKeywords = keywords ++ extKeywords
                        allKeywords
                            .map(k => OneWord(k, 1))
                    }
                    .toDF()
                    .groupBy($"word")
                    .count
                    .filter($"count" > 20)

                val otherWords = inputRdd
                    .filter{ a =>
                        val keywords = if(a.keywords == null) Seq() else JavaConversions.asScalaBuffer(a.keywords)
                        val extKeywords = if(a.extKeywords == null) Seq() else JavaConversions.asScalaBuffer(a.extKeywords)
                        val allKeywords = keywords ++ extKeywords
                        a != null && a.category != null && a.category.size() > 0 && !a.category.contains(specialCategory) && allKeywords.nonEmpty
                    }
                    .flatMap{ a =>
                        val keywords = if(a.keywords == null) Seq() else JavaConversions.asScalaBuffer(a.keywords)
                        val extKeywords = if(a.extKeywords == null) Seq() else JavaConversions.asScalaBuffer(a.extKeywords)
                        val allKeywords = keywords ++ extKeywords
                        allKeywords
                            .map(k => OneWord(k, 1))
                    }
                    .toDF()
                    .groupBy($"word")
                    .count
                    .filter($"count" > 20)

                val result = curWords.toDF().select($"word", $"count".alias("curCount"))
                    .join(otherWords.toDF().select($"word", $"count".alias("specCount")), Seq("word"))
                    .as[JoinTmp]
                    .map{ t =>
                        val score = (t.curCount * 1.0 / t.specCount) * Math.log(t.curCount)
                        WordResult(t.word, t.curCount, t.specCount, score)
                    }
                    .repartition(2)
                    .orderBy($"score".desc)


                result
                    .map{ r =>
                        f"${r.word}\t${r.curCount}\t${r.specCount}\t${r.score}%1.4f"
                    }
                    .write
                    .mode(SaveMode.Overwrite)
                    .text(s"${args("output")}/$engPath")
            }

        spark.stop()
    }
}
