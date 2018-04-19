package com.xiaomi.ad.keyword.marchinggiant.relevance.infofeed

/**
  * Create by liguoyu on 2018-04-19
  * Fun:
  *   输入： ad info ： 广告数据; extension key -> value  : category id 拓展信息数据 ;
  *   注： 如果觉得 ad 数量太大，可添加过滤 code 已注释
  *   1. 从 ad info feed 数据中提取category ,emiCategory ,lda;
  *   2. 对category 添加 extension category ;
  *   3. 根据category score 计算 extension category score
  *   4. 输出结果如 ：adInfoTerm 定义所示。
  */

import com.sun.jersey.core.util.Base64
import com.sun.xml.internal.bind.v2.schemagen.xmlschema.Appinfo
import com.twitter.scalding.Args
import com.xiaomi.miui.ad.keyword.thrift.AdInfo
import com.xiaomi.miui.ad.relevance.common.ThriftSerializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.udf

object adInfoCategory {
  case class help(appId:String, score: Double)
  case class userResult(
                         imei1Md5: String, gCatSeq: Seq[help],  topicSeq: Seq[help], emiCatSeq: Seq[help], kwSeq:Seq[help]
                       )
  case class Category(catId: Int, catName: String, score: Double)
  case class googleCategory(name: String, score: Double)
  case class EmiCategory(name: String, score: Double)
  case class adResult(appId:String, gCatSeq:Seq[googleCategory], emiCatSeq:Seq[help], topicSeq:Seq[help])
  case class LDATopic(topicId: Int, topicName: String, score: Double)
  case class appInfo(
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
  case class adInfoTerm(adId: Long, appinfo: Appinfo, category:Seq[Category], emiCategory: Seq[EmiCategory], lda:Seq[LDATopic])


  def main(args: Array[String]): Unit = {
    val argv = Args(args)
    execute(argv, new SparkConf())
  }


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

    val adAppIdsB = spark.sparkContext.broadcast(adAppIds)

    val appUdf = udf { a: String => adAppIdsB.value.contains(a.toString) }

    val appAndCate = spark.read.parquet(args("input_adinfo"))
      .distinct()
      .select($"adId", $"appInfo", $"levelClassifies", $"emiClassifies", $"adTopicInfo")
      .as[adInfoTerm]
      .map { m =>
        val adId = m.adId
        val appinfo = m.appinfo
//        val appid =
        val google = m.category.flatMap { g =>
          val key = g.catId.toString
          val value = g.score
          val extend = key +: cateExtendB.value.getOrElse(key, Seq())
          val ss = extend.map{ mm =>
            //                        help(mm, value)
            mm -> value
          }
          ss
          //                    help(g.catId.toString, g.score)
        }.filter(f=> f._1 != "0")
          .groupBy(_._1)
          .map{m =>
            val cate = m._1
            val size = m._2.size
            val score = m._2.map{mm=>
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
        adResult(adId.toString, google, emi, lda)
      }
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(args("output"))

    spark.stop()
  }

}
