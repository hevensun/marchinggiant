package com.xiaomi.ad.keyword.marchinggiant.relevance

import com.sun.jersey.core.util.Base64
import com.twitter.scalding.Args
import com.xiaomi.miui.ad.keyword.thrift.AdInfo
import com.xiaomi.miui.ad.relevance.common.ThriftSerializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.udf

/**
  * Created by mi on 2018/4/18.
  * 过滤保留下有广告的app，并对app category进行扩展
  */
object appCategory {

    case class userResult(
                             imei1Md5: String, gCatSeq: Seq[help], topicSeq: Seq[help], emiCatSeq: Seq[help], kwSeq: Seq[help]
                         )

    case class Category(catId: Int, catName: String, score: Double)

    case class googleCategory(name: String, score: Double)

    case class EmiCategory(name: String, score: Double)

    case class adResult(appId: String, gCatSeq: Seq[googleCategory], emiCatSeq: Seq[help], topicSeq: Seq[help])

    case class LDATopic(topicId: Int, topicName: String, score: Double)

    case class Term(appId: Long, keywords: Seq[String], category: Seq[Category], emiCategory: Seq[EmiCategory], lda: Seq[LDATopic])


    def main(args: Array[String]): Unit = {
        val argv = Args(args)
        execute(argv, new SparkConf())
    }

    case class help(appId: String, score: Double)

    def execute(args: Args, sparkConf: SparkConf) = {
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._

        val adAppIds = spark.sparkContext.textFile(args("adInfoPath"), 500).map(line =>
            ThriftSerializer.deserialize(Base64.decode(line), classOf[AdInfo]))
            .filter(e => (e.isSetAppInfo && e.getAppInfo.isSetAppId && e.getAppInfo.getAppId > 0
                && e.getAdId > 0)).map(e => {
            val appinfo = e.getAppInfo
            val appId = appinfo.getAppId.toString
            (appId)
        }).distinct().collect().toSet

        val cateExtend = spark.read.text(args("input_cateExtend"))
            .map { m =>
                val ss = m.getAs[String]("value")
                if (ss.split("\t").length > 1) {
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

        val adAppIdsB = spark.sparkContext.broadcast(adAppIds)

        val appUdf = udf { a: String => adAppIdsB.value.contains(a.toString) }

        val appAndCate = spark.read.parquet(args("input_app"))
            .filter(appUdf($"appId"))
            .distinct()
            .select($"appId", $"keywords", $"category", $"emiCategory", $"lda")
            .as[Term]
            .map { m =>
                val appId = m.appId
                val google = m.category.flatMap { g =>
                    val key = g.catId.toString
                    val value = g.score
                    val extend = key +: cateExtendB.value.getOrElse(key, Seq())
                    val ss = extend.map { mm =>
                        mm -> value
                    }
                    ss
                }.filter(f => f._1 != "0")
                    .groupBy(_._1)
                    .map { m =>
                        val cate = m._1
                        val size = m._2.size
                        val score = m._2.map { mm =>
                            mm._2
                        }.sum
                        googleCategory(cate, score / size)
                    }.toSeq
                val emi = m.emiCategory.map { e =>
                    help(e.name, e.score)
                }
                val lda = m.lda.map { l =>
                    help(l.topicId.toString, l.score)
                }
                adResult(appId.toString, google, emi, lda)
            }
            .repartition(1)
            .write
            .mode(SaveMode.Overwrite)
            .parquet(args("output"))

        spark.stop()
    }
}
