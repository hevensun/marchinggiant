package com.xiaomi.ad.keyword.marchinggiant.relevance.infofeed

import com.twitter.scalding.Args
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.math.sqrt

/**
  * create by liguoyu on 2018-05-22
  * key words relevence by jaccard distance
  */

object userKeyWordsJaccardReleWithAd {
  case class cosResult1(appId: String, cosSim: Double)

  case class cosResult3(imei:String, jaccardKeywords: Seq[cosResult1])

  case class cosResult2(imei:String, cosSim:Seq[cosResult1])

  case class userResult(
                         imei1Md5: String, kwSeq:Seq[help]
                       )
  case class adResult(appId: Long, keyWords: Seq[String])

  case class help(cate: String, score: Double)

  def main(args: Array[String]): Unit = {
    val argv = Args(args)
    execute(argv, new SparkConf())
  }

  // index 表示相似度计算类型 （0-2：google, emi , lda）
  def cmpCosin(catSeq: Seq[help], adInfoMap: Map[String, Seq[Seq[String]]], index: Int = 0): Seq[cosResult1] = {
    val userGooList = catSeq.map { mm =>
      mm.cate
    }.toSet.toList
    val sum3 = catSeq.map { in =>
      in.score * in.score
    }.sum

    val google = catSeq.map { mm =>
      mm.cate -> mm.score / sqrt(sum3)
    }.toMap

    val adAppGoole = adInfoMap.map { app =>
      val appId = app._1
      val adCate = app._2(index).map { add =>
        add.split("\t")(0) -> add.split("\t")(1).toDouble
      }.toMap

      val sum1 = userGooList.map { in =>
        adCate.getOrElse(in, 0.0) * google.getOrElse(in, 0.0)
      }.sum
      cosResult1(appId, sum1)

    }.filter(f => f.cosSim > 0.0).toSeq
      .sortBy(s => -s.cosSim)
    adAppGoole
  }


  def cmpJecardDis(catSeq: Seq[help], adInfoMap: Map[String, Seq[Seq[String]]], tpc: Int): Seq[cosResult1] = {
    val userGooList = catSeq.map { mm =>
      mm.cate
    }.toSet

    val adAppGoole = adInfoMap.map { app =>
      val appId = app._1
      val adCate = app._2(tpc).toSet
      val intersectNum = userGooList.intersect(adCate).size.toDouble
      val unionNum = userGooList.union(adCate).size.toDouble
      val distance = intersectNum/unionNum
      cosResult1(appId, distance)
    }.filter(f => f.cosSim > 0.0).toSeq
      .sortBy(s => -s.cosSim)
    adAppGoole
  }

  def execute(args: Args, sparkConf: SparkConf) = {
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._
    //    // 转换为 cate 集合, 计算 A交B 和 A 并 B

    val app = spark.read.parquet(args("input2"))
      .as[adResult]
      .map{m =>
        val appId = m.appId
        val keywords = m.keyWords
        appId.toString -> Seq(keywords)
      }.collect().toMap

    val appKeywordsB = spark.sparkContext.broadcast(app)

    val user = spark.read.parquet(args("input1"))
      .as[userResult]
      .repartition(1000)
      //.filter(f => f.gCatSeq.size < 500)
      .map{ m =>
        val keywords = cmpJecardDis(m.kwSeq, appKeywordsB.value, 0)
        cosResult3(m.imei1Md5, keywords)
      }.repartition(200)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(args("output"))

    spark.stop()
  }
}
