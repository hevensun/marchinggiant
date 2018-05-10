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
import org.apache.spark.sql.functions.udf

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

    val data = spark.read.parquet(args("input_matrix"))
    val appPackageTopN = data.filter($"sourceId"===3).select($"entityKey")
      .map(r=>(r.getAs[String]("entityKey").split('|').last,1)).rdd.reduceByKey(_ + _).sortBy(-_._2).take(args("thread").toInt).map(r=>r._1).toSet
    println("appPackage size:"+appPackageTopN.size)

    val appIdTopN = data.filter($"sourceId"===5&&($"actionId"===1||$"actionId"===4)&&$"text".isNotNull).select($"text")
      .map(r=>(r.getAs[String]("text"),1)).rdd.reduceByKey(_ + _).sortBy(-_._2).take(args("thread").toInt).map(r=>r._1).toSet
    println("appid size:"+appIdTopN.size)
    val appIdTopNBc = spark.sparkContext.broadcast(appIdTopN)
    val appPackageTopNBc = spark.sparkContext.broadcast(appPackageTopN)
    // udf
    val datafilter = udf{(packageName:String, appId:String)=>
      appIdTopNBc.value.contains(appId.toString)||appPackageTopNBc.value.contains(packageName)
    }
    val appNew = spark.read.parquet(args("input"))
      .dropDuplicates(Seq("appId"))
      .filter(datafilter($"appId", $"packageName"))
    println("--------------------------------------------------------------------------------------------")
    println("result size:"+appNew.count())
    println("--------------------------------------------------------------------------------------------")
    appNew.select($"appId", $"packageName", $"keywords", $"category", $"emiCategory", $"lda")
      .as[AppExtensioNew]
      .repartition(1)
      .write.mode(SaveMode.Overwrite)
      .parquet(args("output"))
    spark.stop()
  }
}
