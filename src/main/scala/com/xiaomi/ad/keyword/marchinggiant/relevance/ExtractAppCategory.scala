package com.xiaomi.ad.keyword.marchinggiant.relevance

/**
  * create by liguoyu on 2018-04-23
  * add changed on 2018-05-10
  * 统计matrix 中sourceid===3 和 sourceId === 5&&(actionId===1||actionId===4)的高频UV 的 app
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
import org.apache.spark.sql.functions._
import collection.JavaConverters._
import org.apache.spark.sql.functions.udf

import scala.util.Try

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
    val triml = udf{(entity:String)=>
      Try(entity.split('|').last).getOrElse(null)
    }
    println("app pack thread"+args("apppackAgeThread")+"app id thread:"+ args("appIdUvThread"))
    val appPackage = data.filter($"sourceId"===3)
      .select(triml($"entityKey").alias("appPackage"),$"imei1Md5")
      .groupBy($"appPackage", $"imei1Md5")
      .count()
      .select($"appPackage", lit(1).alias("appPackageUv"))
      .groupBy($"appPackage")
      .agg(sum($"appPackageUv").alias("appPackageUv"))
      .filter($"appPackageUv" > args("apppackAgeThread").toInt)
      .select($"appPackage").map(r=>r.getAs[String]("appPackage")).collect().toSet

    val appIds = data.filter($"sourceId"===5&&($"actionId"===1||$"actionId"===4)&&$"text".isNotNull)
      .select($"text".alias("appId"), $"imei1Md5")
      .groupBy($"appId", $"imei1Md5")
      .count()
      .select($"appId", lit(1).alias("appIdUv"))
      .groupBy($"appId")
      .agg(sum($"appIdUv").alias("appIdUv")).filter($"appIdUv" > args("appIdUvThread").toInt)
      .select("appId").map(r=>r.getAs[String]("appId")).collect().toSet

    val appIdsBc = spark.sparkContext.broadcast(appIds)
    val appPackageBc = spark.sparkContext.broadcast(appPackage)

    val filterApp = udf{(appId: String, packageName:String)=>
      appIdsBc.value.contains(appId)||appPackageBc.value.contains(packageName)
    }

    val appNew = spark.read.parquet(args("input"))
      .dropDuplicates(Seq("appId"))
      .filter(filterApp($"appId", $"packageName"))
      .select($"appId", $"packageName", $"keywords", $"category", $"emiCategory", $"lda")
      .as[AppExtensioNew]
      .repartition(1)
      .write.mode(SaveMode.Overwrite)
      .parquet(args("output"))
//    println("app pack thread"+args("apppackAgeThread")+"app id thread:"+ args("appIdUvThread"))
//    println("app cate id number"+appNewIds.size+"matrix app number:"+appIds.size)
//    println("app id 交集数量："+ appNewIds.intersect(appIds).size)
//    println("app cate package number"+appNewPackage.size+"matrix app package number:"+appPackage.size)
//    println("app package 交集数量："+ appNewPackage.intersect(appPackage).size)
    println("Task Finished!!!")
    spark.stop()
  }
}
