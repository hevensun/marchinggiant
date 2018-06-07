package com.xiaomi.ad.keyword.marchinggiant.adinfo

import com.sun.jersey.core.util.Base64
import com.twitter.scalding.Args
import com.xiaomi.miui.ad.keyword.thrift.AdInfo
import com.xiaomi.miui.ad.relevance.common.ThriftSerializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.udf

import scala.util.Try

/**
  * Created by wangzhijun.
  */

object  GetAdEnrichInfoJob {

    case class Category(catId: Int, catName: String, score: Double)
    case class googleCategory(name: String, score: Double)
    case class EmiCategory(name: String, score: Double)
    case class LDATopic(topicId: Int, topicName: String, score: Double)
    case class Term(appId: Long,packageName: String, category: Seq[Category], emiCategory: Seq[EmiCategory], lda: Seq[LDATopic],level1CategoryName: String,level2CategoryName: String)

    def main(args: Array[String]): Unit = {
        val argv = Args(args)
        execute(argv, new SparkConf())
    }

    case class help(appId: String, score: Double)

    def execute(args: Args, sparkConf: SparkConf) = {
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._

        /*
        val adAppIds = spark.sparkContext.textFile(args("adInfoPath"), 500).map(line =>
            ThriftSerializer.deserialize(Base64.decode(line), classOf[AdInfo]))
            .filter(e => (e.isSetAppInfo && e.getAppInfo.isSetAppId && e.getAppInfo.getAppId > 0
                && e.getAdId > 0)).map(e => {
            val appinfo = e.getAppInfo
            val appId = appinfo.getAppId.toString
            (appId)
        }).distinct().collect().toSet
        */


        val ss = spark.sparkContext.textFile(args("adInfoPath"), 500).map(line =>
            ThriftSerializer.deserialize(Base64.decode(line), classOf[AdInfo]))
            .filter(e => (e.isSetAppInfo && e.getAppInfo.isSetAppId && e.getAppInfo.getAppId > 0
                && e.getAdId > 0)).map(e => {
            val appinfo = e.getAppInfo
            val appId = appinfo.getAppId.toString
            appId
        }).distinct()

        val adAppIds = ss.collect().toSet

        ss.saveAsTextFile(args("output_all_ads"))

        val adAppIdsB = spark.sparkContext.broadcast(adAppIds)
        val appUdf = udf { a: String => adAppIdsB.value.contains(a.toString)}

        val appAndCate = spark.read.parquet(args("input_app"))
            .filter(appUdf($"appId"))
            .select($"appId", $"packageName",$"category", $"emiCategory", $"lda",$"level1CategoryName",$"level2CategoryName")
            .as[Term]
            .map { m =>
                val appId = m.appId.toString
                val pkgname = m.packageName
                val l1name = m.level1CategoryName
                val l2name = m.level2CategoryName

                val googleCategoryStr = m.category.map{ t => t.catId+":"+t.catName + ":" + t.score.toString}.mkString("#")
                val emiStr = m.emiCategory.map { e =>
                    e.name + ":" + e.score.toString
                }.mkString("#")
                val lda = m.lda.map { l =>
                    l.topicId + ":" + l.score.toString
                }.mkString("#")

                s"$appId\t$pkgname\t$googleCategoryStr\t$emiStr\t$lda\t$l1name\t$l2name"
            }
            .repartition(1)
            .write
            .mode(SaveMode.Overwrite)
            .text(args("output"))

        spark.stop()
    }
}
