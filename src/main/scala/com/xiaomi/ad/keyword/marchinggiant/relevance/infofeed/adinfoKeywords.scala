package com.xiaomi.ad.keyword.marchinggiant.relevance.infofeed

import com.twitter.scalding.Args
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * create by liguoyu on 2018-05-22
  * 生成 ad keywords  向量
  */

object adinfoKeywords {
  case class help(cateId:String, score: Double)
  case class userResult(
                         imei1Md5: String, gCatSeq: Seq[help],  topicSeq: Seq[help], emiCatSeq: Seq[help], kwSeq:Seq[help]
                       )
  case class Category(id: Int, catName: String, score: Double)
  case class EmiCategory(catName: String, score: Double)
  case class adResult(appId:String, gCatSeq:Seq[help], emiCatSeq:Seq[help], topicSeq:Seq[help])
  case class LDATopic(topicId: Int, score: Double, topicDesc: String)
  case class AppInfo(
                      packageName: String,
                      appId: Long,
                      level1Category: String,
                      level2Category: String,
                      displayName: String,
                      keyWords: Seq[String],
                      levelClassifies: Seq[Category],
                      keywordsClassifies: Seq[Category],
                      keywordsTopicInfo: Seq[LDATopic],
                      introduction: String,
                      brief: String
                    )
  case class adInfoTerm(adId: Long, appinfo: AppInfo)
  case class adWithAppTerm(adId: Long, appId: Long, app_keywords: Seq[String])

  case class KeyWords(appId: Long, keyWords: Seq[String])

  def main(args: Array[String]): Unit = {
    val argv = Args(args)
    val sparkConf = new SparkConf()
    //    sparkConf.setMaster("local")
    execute(argv, sparkConf)
  }


  def execute(args: Args, sparkConf: SparkConf) = {
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    val cateExtend = spark.read.text(args("input_cateExtend"))
      .map{ m =>
        val ss = m.getAs[String]("value")
        if ( ss.split("\t").length > 1) {
          val key = ss.split("\t")(0)
          val value = ss.split("\t").drop(1).toSeq
          key -> value
        } else {
          "###" -> List("***")
        }
      }.filter(f => f._1 != "###")
      .collect()
      .toMap

    val cateExtendB = spark.sparkContext.broadcast(cateExtend)

    val cateThread = spark.sparkContext.broadcast(args("CateThread").toDouble)

    val appAndCate = spark.read.parquet(args("input_adinfo"))
      .select($"adId", $"appInfo")
    val appAndCateGroup = appAndCate.as[adInfoTerm].filter($"appInfo".isNotNull)
      .map { m =>
        val adId = m.adId
        val appId = m.appinfo.appId
        // keywords
        val app_keywords = if(m.appinfo.keyWords == null) Seq() else m.appinfo.keyWords
        (appId, adWithAppTerm(adId, appId, app_keywords))
      }.groupByKey(_._1)

    val ad_keywords = appAndCateGroup.mapGroups{case (key, value) =>
      val appId = key
      val data = value
      val keywords = data.flatMap(terms=>terms._2.app_keywords).toSet.toSeq
      KeyWords(appId, keywords)
    }

    ad_keywords.as[KeyWords]
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(args("output"))

    spark.stop()
  }

}
