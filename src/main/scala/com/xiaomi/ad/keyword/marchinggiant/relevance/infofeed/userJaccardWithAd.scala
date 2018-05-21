package com.xiaomi.ad.keyword.marchinggiant.relevance.infofeed

import com.twitter.scalding.Args
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.math.sqrt

object userJaccardWithAd {
  case class cosResult1(appId: String, cosSim: Double)

  case class cosResult3(imei:String, cosSimG:Seq[cosResult1], cosSimE:Seq[cosResult1], cosSimL:Seq[cosResult1])

  case class cosResult2(imei:String, cosSim:Seq[cosResult1])

  case class userResult(
                         imei1Md5: String, gCatSeq: Seq[help],  topicSeq: Seq[help], emiCatSeq: Seq[help], kwSeq:Seq[help]
                       )
  case class adResult(appId:String, gCatSeq:Seq[help], emiCatSeq:Seq[help], topicSeq:Seq[help])

  case class help(cateId:String, score: Double)

  def main(args: Array[String]): Unit = {
    val argv = Args(args)
    execute(argv, new SparkConf())
  }

  // index 表示相似度计算类型 （0-2：google, emi , lda）
  def cmpCosin(catSeq: Seq[help], adInfoMap: Map[String, Seq[Seq[String]]], index: Int = 0): Seq[cosResult1] = {
    val userGooList = catSeq.map { mm =>
      mm.cateId
    }.toSet.toList
    val sum3 = catSeq.map { in =>
      in.score * in.score
    }.sum

    val google = catSeq.map { mm =>
      mm.cateId -> mm.score / sqrt(sum3)
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
      mm.cateId
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
        val google = m.gCatSeq.map(r=>r.cateId)
        val emiCate = m.emiCatSeq.map(r=>r.cateId)
        val ldaTopic = m.topicSeq.map(r=>r.cateId)
        appId -> Seq(google, emiCate, ldaTopic)
      }.collect().toMap


    val appAndCateB = spark.sparkContext.broadcast(app)

    val user = spark.read.parquet(args("input1"))
      .as[userResult]
      .repartition(1000)
      .filter(f => f.gCatSeq.size < 500)
      .map{ m =>
        val adAppGoole = cmpJecardDis(m.gCatSeq, appAndCateB.value, 0)
        val adAppEmi = cmpJecardDis(m.emiCatSeq, appAndCateB.value, 1)
        val adAppLda = cmpJecardDis(m.topicSeq, appAndCateB.value, 2)

        cosResult3(m.imei1Md5, adAppGoole, adAppEmi, adAppLda)
      }.repartition(200)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(args("output"))

    spark.stop()
  }
}
