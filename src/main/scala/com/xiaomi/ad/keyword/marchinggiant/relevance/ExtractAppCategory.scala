package com.xiaomi.ad.keyword.marchinggiant.relevance

/**
  * create by liguoyu on 2018-04-23
  * 从app_extension 数据中提取其中的
  *           appId ,
  *           apppackagename,
  *           google category,
  *           lda topic ,
  *           emi category,
  *           keywords
  * 目的：减小下一步处理的数据加载压力，降低内存占用
  */

import com.twitter.scalding.Args
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import collection.JavaConverters._

object ExtractAppCategory {
  case class Category(catId: Int, catName: String, score: Double)
  case class EmiCategory(name: String, score: Double)
  case class LdaTopic(topicId: Int, topicName: String, score: Double)
  case class AppExtensioNew(
                             appId: Long,
                             packageName: String,
                             keywords: Seq[String],
                             category: Seq[Category],
                             emiCategory: Seq[EmiCategory],
                             lda: Seq[LdaTopic]
                           )

  def main(args: Array[String]): Unit = {
    val inputParems = Args(args)
    val conf = new SparkConf()
    excute(inputParems, conf)
  }

  def excute(args: Args, sparkConf: SparkConf): Unit ={
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    val appNew = spark.read.parquet(args("input"))
      .dropDuplicates(Seq("appId"))
      .select($"appId", $"packageName", $"keywords", $"category", $"emiCategory", $"lda")
      .as[AppExtensioNew]
      .repartition(1)
      .write.mode(SaveMode.Overwrite)
      .parquet(args("output"))
    spark.stop()
  }
}
