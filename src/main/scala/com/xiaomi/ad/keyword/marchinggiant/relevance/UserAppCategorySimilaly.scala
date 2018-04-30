package com.xiaomi.ad.keyword.marchinggiant.relevance

import com.twitter.scalding.Args
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.math.sqrt

object UserAppCategorySimilaly {

  case class cosResult1(appId: String, cosSim: Double)

  case class cosResult3(imei:String, cosSimG:Seq[cosResult1], cosSimE:Seq[cosResult1], cosSimL:Seq[cosResult1])

  case class cosResult2(imei:String, cosSim:Seq[cosResult1])

  case class userResult(
                         imei1Md5: String, gCatSeq: Seq[help],  topicSeq: Seq[help], emiCatSeq: Seq[help], kwSeq:Seq[help]
                       )
  case class adResult(appId:String, gCatSeq:Seq[help], emiCatSeq:Seq[help], topicSeq:Seq[help])

  case class help(appId:String, score: Double)

  case class userResultAppExtension(
                         imei1Md5: String,
                         gCatSeq: Seq[help],
                         topicSeq: Seq[help],
                         emiCatSeq: Seq[help],
                         kwSeq:Seq[help],
                         appGoogleCatSeq: Seq[help],
                         appTopicSeq: Seq[help],
                         appEmiCatSeq: Seq[help],
                         appKeyWordsSeq:Seq[String]
                       )

  case class adResultAppExtension(appId:String, gCatSeq:Seq[help], emiCatSeq:Seq[help], topicSeq:Seq[help],
                      appGoogleCatSeq: Seq[help],
                      appTopicSeq: Seq[help],
                      appEmiCatSeq: Seq[help],
                      appKeyWordsSeq:Seq[String])

  case class cosResult3AppExtension(
                         imei:String,
                         cosSimG:Seq[cosResult1],
                         cosSimE:Seq[cosResult1],
                         cosSimL:Seq[cosResult1],
                         cosSimAppG: Seq[cosResult1],
                         cosSimAppE: Seq[cosResult1],
                         cosSimAppL: Seq[cosResult1]
                       )

  def main(args: Array[String]): Unit = {
    val argv = Args(args)
    execute(argv, new SparkConf())
  }

  // index 表示相似度计算类型 （0-2: google, emi , lda）
  def cmpCosin(catSeq: Seq[help], adInfoMap: Map[String, Seq[Seq[String]]], index: Int=0): Seq[cosResult1] = {
    val userGooList = catSeq.map { mm =>
      mm.appId
    }.toSet.toList
    val sum3 = catSeq.map { in =>
      in.score * in.score
    }.sum

    val google = catSeq.map { mm =>
      mm.appId -> mm.score / sqrt(sum3)
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

  def execute(args: Args, sparkConf: SparkConf) = {
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    val app = spark.read.parquet(args("input2"))
      .as[adResult]
      .map { m =>
        val sumG = m.gCatSeq.map { item =>
          item.score * item.score
        }.sum

        val appId = m.appId
        val google = m.gCatSeq.map { mm =>
          val normalScore = mm.score / sqrt(sumG)
          mm.appId + "\t" + normalScore.toString
        }

        val sumE = m.emiCatSeq.map { item =>
          item.score * item.score
        }.sum

        val emi = m.emiCatSeq.map { mmm =>
          val normalScore = mmm.score / sqrt(sumE)
          mmm.appId + "\t" + normalScore.toString
        }

        val sumL = m.topicSeq.map { item =>
          item.score * item.score
        }.sum

        val lda = m.topicSeq.map { mm =>
          val normalScore = mm.score / sqrt(sumL)
          mm.appId + "\t" + normalScore.toString
        }

        appId -> Seq(google, emi, lda)
      }.collect().toMap

    val appAndCateB = spark.sparkContext.broadcast(app)

    val user = spark.read.parquet(args("input1"))
      .as[userResultAppExtension]
      .repartition(500)
      .filter(f => f.gCatSeq.size < 500)
      .map{ m =>
        //user google category
        val adAppGoole = cmpCosin(m.gCatSeq, appAndCateB.value, 0)
        val adAppEmi = cmpCosin(m.emiCatSeq, appAndCateB.value, 1)
        val adAppLda = cmpCosin(m.topicSeq, appAndCateB.value, 2)
        // user app google category with ad category cos similaly caculate
        val adAppExtensionGoogleSim = cmpCosin(m.appGoogleCatSeq, appAndCateB.value, 0)
        val adAppExtensionEmiSim = cmpCosin(m.appEmiCatSeq, appAndCateB.value, 1)
        val appExtensionLdaTopicSim = cmpCosin(m.appTopicSeq, appAndCateB.value, 2)

//        cosResult3AppExtension(m.imei1Md5, Seq(), Seq(), Seq(), Seq(), Seq(), appExtensionLdaTopicSim)
        cosResult3AppExtension(m.imei1Md5, adAppGoole, adAppEmi, adAppLda, adAppExtensionGoogleSim, adAppExtensionEmiSim, appExtensionLdaTopicSim)
      }
      .filter(r => r.cosSimG.nonEmpty || r.cosSimE.nonEmpty || r.cosSimL.nonEmpty)
      .repartition(400)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(args("output"))
    println("similaly task finished!")
    spark.stop()
  }

}
