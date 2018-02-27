package com.xiaomi.ad.keyword.marchinggiant.news

import com.twitter.scalding.Args
import com.xiaomi.data.spec.log.profile.ArticleInfo
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.xiaomi.data.commons.spark.HdfsIO._

import scala.collection.JavaConversions._
import scala.util.Try

/**
  * Created by cailiming on 17-11-29.
  */
object ExtractNews {
    def main(args: Array[String]): Unit = {
        val argv = Args(args)
        val sparkConf = new SparkConf()
        execute(argv, sparkConf)
    }

    def execute(args: Args, sparkConf: SparkConf) = {
        val spark = SparkSession
            .builder()
            .config(sparkConf)
            .getOrCreate()

        import spark.implicits._

        val ans = spark
            .sparkContext
            .thriftSequenceFile(args("input"), classOf[ArticleInfo])
            .filter(a => a.topics != null && a.topics.contains("汽车"))
            .map { article =>
                News(article.getTitle, article.getSummary, article.getBody, article.getUserTags)
            }

        ans.map { n =>
            val title = Try(n.title.replace("\n", " ")).getOrElse("")
            val summary = Try(n.summary.replace("\n", " ")).getOrElse("")
            val body = Try(n.body.replace("\n", " ")).getOrElse("")

            s"$title\n$summary\n$body\n${Try(n.userTags.mkString(";")).getOrElse("")}"
        }
        .repartition(10)
        .toDF()
        .write
        .mode(SaveMode.Overwrite)
        .text(args("output"))

        spark.stop()
    }

}
