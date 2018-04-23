package com.xiaomi.ad.keyword.marchinggiant.relevance
import scala.math.sqrt
import com.twitter.scalding.Args
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by mi on 2018/4/17.
  * 将app和user进行join，求得余弦相似度
  */
object testJoin {
    case class cosResult1(appId: String, cosSim: Double)
    
    case class cosResult3(imei:String, cosSimG:Seq[cosResult1], cosSimE:Seq[cosResult1], cosSimL:Seq[cosResult1])
    
    case class cosResult2(imei:String, cosSim:Seq[cosResult1])
    
    case class userResult(
                             imei1Md5: String, gCatSeq: Seq[help],  topicSeq: Seq[help], emiCatSeq: Seq[help], kwSeq:Seq[help]
                         )
    case class adResult(appId:String, gCatSeq:Seq[help], emiCatSeq:Seq[help], topicSeq:Seq[help])
    
    case class help(appId:String, score: Double)
    
    def main(args: Array[String]): Unit = {
        val argv = Args(args)
        execute(argv, new SparkConf())
    }
    def execute(args: Args, sparkConf: SparkConf) = {
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._
        
        val app = spark.read.parquet(args("input2"))
            .as[adResult]
            .map{ m =>
                val appId = m.appId
                val google = m.gCatSeq.map { mm =>
                    mm.appId + "\t" + mm.score.toString
                }
                val emi = m.emiCatSeq.map { mmm =>
                    mmm.appId + "\t" + mmm.score.toString
                }
                val lda = m.topicSeq.map{ mm =>
                    mm.appId + "\t" + mm.score.toString
                }
                appId -> Seq(google, emi, lda)
            }.collect().toMap
        
        val appAndCateB = spark.sparkContext.broadcast(app)
        
        val user = spark.read.parquet(args("input1"))
            .as[userResult]
            .repartition(500)
            .filter(f => f.gCatSeq.size < 500)
            .map{ m =>
                val userGooList = m.gCatSeq.map { mm =>
                    mm.appId
                }.toSet
                val google = m.gCatSeq.map{ mm =>
                    mm.appId -> mm.score
                }.toMap
                val adAppGoole = appAndCateB.value.map { app =>
                    val appId = app._1
                    val appGooList = app._2(0).map{ mm =>
                        mm.split("\t")(0)
                    }.toSet
//                    val interSet = appGooList.intersect(userGooList)
                    val unionSet = appGooList.union(userGooList)
                    val adCate = app._2(0).map{ add =>
                        add.split("\t")(0) -> add.split("\t")(1).toDouble
                    }.toMap
                
                    val sum1 = unionSet.toList.map{ in =>
                        adCate.getOrElse(in, 0.0) * google.getOrElse(in, 0.0)
                    }.sum
                
                    val sum2 = unionSet.toList.map{ in =>
                        adCate.getOrElse(in, 0.0) * adCate.getOrElse(in, 0.0)
                    }.sum
                
                    val sum3 = unionSet.toList.map{ in =>
                        google.getOrElse(in, 0.0) * google.getOrElse(in, 0.0)
                    }.sum
                    cosResult1(appId, sum1 / (sqrt(sum2) * sqrt(sum3)))
                }.filter(f => f.cosSim > 0.0).toSeq
                .sortBy(s => -s.cosSim)
    
                val userEmiList = m.emiCatSeq.map { mm =>
                    mm.appId
                }.toSet
                val emi = m.emiCatSeq.map{ mm =>
                    mm.appId -> mm.score
                }.toMap
                val adAppEmi = appAndCateB.value.map { app =>
                    val appId = app._1
                    val appEmiList = app._2(1).map{ mm =>
                        mm.split("\t")(0)
                    }.toSet
//                    val interSet = appEmiList.intersect(userEmiList)
                    val unionSetemei = appEmiList.union(userEmiList)
                    val adCate = app._2(1).map{ add =>
                        add.split("\t")(0) -> add.split("\t")(1).toDouble
                    }.toMap
        
                    val sum1 = unionSetemei.toList.map{ in =>
                        adCate.getOrElse(in, 0.0) * emi.getOrElse(in, 0.0)
                    }.sum
        
                    val sum2 = unionSetemei.toList.map{ in =>
                        adCate.getOrElse(in, 0.0) * adCate.getOrElse(in, 0.0)
                    }.sum
        
                    val sum3 = unionSetemei.toList.map{ in =>
                        emi.getOrElse(in, 0.0) * emi.getOrElse(in, 0.0)
                    }.sum
                    cosResult1(appId, sum1 / (sqrt(sum2) * sqrt(sum3)))
                }.filter(f => f.cosSim > 0.0).toSeq
                .sortBy(s => -s.cosSim)
    
                val userLdaList = m.topicSeq.map { mm =>
                    mm.appId
                }.toSet
                val lda = m.topicSeq.map{ mm =>
                    mm.appId -> mm.score
                }.toMap
                val adAppLda = appAndCateB.value.map { app =>
                    val appId = app._1
                    val appLdaList = app._2(2).map{ mm =>
                        mm.split("\t")(0)
                    }.toSet
//                    val interSet = appLdaList.intersect(userLdaList)
                    val unionSetLda = appLdaList.union(userLdaList)
                    val adCate = app._2(2).map{ add =>
                        add.split("\t")(0) -> add.split("\t")(1).toDouble
                    }.toMap
        
                    val sum1 = unionSetLda.toList.map{ in =>
                        adCate.getOrElse(in, 0.0) * lda.getOrElse(in, 0.0)
                    }.sum
        
                    val sum2 = unionSetLda.toList.map{ in =>
                        adCate.getOrElse(in, 0.0) * adCate.getOrElse(in, 0.0)
                    }.sum
        
                    val sum3 = unionSetLda.toList.map{ in =>
                        lda.getOrElse(in, 0.0) * lda.getOrElse(in, 0.0)
                    }.sum
                    cosResult1(appId, sum1 / (sqrt(sum2) * sqrt(sum3)))
                }.filter(f => f.cosSim > 0.0).toSeq
                .sortBy(s => -s.cosSim)
                
                cosResult3(m.imei1Md5, adAppGoole, adAppEmi, adAppLda)
            }
            .filter(r => r.cosSimG.nonEmpty || r.cosSimE.nonEmpty || r.cosSimL.nonEmpty)
            .repartition(400)
            .write
            .mode(SaveMode.Overwrite)
            .parquet(args("output"))
        
        spark.stop()
    }
}
