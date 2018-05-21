package com.xiaomi.ad.keyword.marchinggiant.relevance

import com.twitter.scalding.Args
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.math.sqrt

object UserAppCategoryJaccard {
  case class Term(cate: String, score: Double)
  case class cosResult1(appId: String, cosSim: Double)

  case class cosResult3(imei: String, cosSimG: Seq[cosResult1], cosSimE: Seq[cosResult1], cosSimL: Seq[cosResult1])

  case class cosResult3WithApp(imei: String, cosSimG: Seq[cosResult1], cosSimE: Seq[cosResult1], cosSimL: Seq[cosResult1],appCosSimG: Seq[cosResult1], appCosSimE: Seq[cosResult1], appCosSimL: Seq[cosResult1])

  case class cosResult2(imei: String, cosSim: Seq[cosResult1])

  case class userResult(
                         imei1Md5: String, gCatSeq: Seq[help], topicSeq: Seq[help], emiCatSeq: Seq[help], kwSeq: Seq[help]
                       )

  case class ResultAppExtension(
                                 imei1Md5: String,
                                 gCatSeq: Seq[Term],
                                 topicSeq: Seq[Term],
                                 emiCatSeq: Seq[Term],
                                 kwSeq: Seq[Term],
                                 appGoogleCatSeq: Seq[Term],
                                 appTopicSeq: Seq[Term],
                                 appEmiCatSeq: Seq[Term],
                                 appKeyWordsSeq: Seq[String]
                               )

  case class adResult(appId: String, gCatSeq: Seq[help], emiCatSeq: Seq[help], topicSeq: Seq[help])

  case class help(appId: String, score: Double)

  def main(args: Array[String]): Unit = {
    val argv = Args(args)
    val conf = new SparkConf()
    execute(argv, conf)
  }

  def cmpCosin(catSeq: Seq[Term], adInfoMap: Map[String, Seq[Seq[String]]], tpc: Int): Seq[cosResult1] = {
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
      val adCate = app._2(tpc).map { add =>
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

  def cmpJecardDis(catSeq: Seq[Term], adInfoMap: Map[String, Seq[Seq[String]]], tpc: Int): Seq[cosResult1] = {
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

    // 转换为 cate 集合, 计算 A交B 和 A 并 B
    val app = spark.read.parquet(args("input2"))
      .as[adResult]
      .map{m =>
        val appId = m.appId
        val google = m.gCatSeq.map{r=>r.appId}
        val emiCate = m.emiCatSeq.map(r=>r.appId)
        val ldaTopic = m.topicSeq.map(r=>r.appId)
        appId -> Seq(google, emiCate, ldaTopic)
      }.collect().toMap

    val appAndCateB = spark.sparkContext.broadcast(app)
    println("step one finished")
    val user = spark.read.parquet(args("input1"))
      .as[ResultAppExtension]
      .repartition(5000)
      .filter(f => f.gCatSeq.size < 500&&f.appGoogleCatSeq.size < 500)
      .map { m =>
        val Goole = cmpJecardDis(m.gCatSeq, appAndCateB.value, 0)
        val Emi = cmpJecardDis(m.emiCatSeq, appAndCateB.value, 1)
        val Lda = cmpJecardDis(m.topicSeq, appAndCateB.value, 2)

        val appGoogle = cmpJecardDis(m.appGoogleCatSeq, appAndCateB.value, 0)
        val appEmi = cmpJecardDis(m.appEmiCatSeq, appAndCateB.value, 1)
        val appLda = cmpJecardDis(m.appTopicSeq, appAndCateB.value, 2)

        cosResult3WithApp(m.imei1Md5, Goole, Emi, Lda, appGoogle, appEmi, appLda)
      }
    val fina_re = user.filter(r => r.cosSimG.nonEmpty || r.cosSimE.nonEmpty || r.cosSimL.nonEmpty||r.appCosSimE.nonEmpty||r.appCosSimG.nonEmpty||r.appCosSimL.nonEmpty)
      .repartition(1000)
    fina_re.write
      .mode(SaveMode.Overwrite)
      .parquet(args("output"))
    spark.stop()
    println("Task finished")
  }

}
