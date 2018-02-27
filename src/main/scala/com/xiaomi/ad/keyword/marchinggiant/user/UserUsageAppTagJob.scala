package com.xiaomi.ad.keyword.marchinggiant.user

import com.twitter.scalding.Args
import com.xiaomi.ad.keyword.marchinggiant.ner.BehaviorTag
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.util.Try

/**
  * Created by cailiming on 18-1-31.
  */
object UserUsageAppTagJob {
    def main(args: Array[String]): Unit = {
        val argv = Args(args)
        val sparkConf = new SparkConf()
        execute(argv, sparkConf)
    }

    def execute(args: Args, sparkConf: SparkConf) = {
        val spark = SparkSession
            .builder()
            .config(sparkConf)
            .getOrCreate()

        import spark.implicits._


        val appUsageDF = spark.read.parquet(args("appUsage"))
            .as[BehaviorTag]

        val appTagsMap = spark.read.parquet(args("appTags"))
            .as[AppTags]
            .map { t =>
                t.packageName -> t.tags
            }
            .collect()
            .toMap

        val appTagsBroadCast = spark.sparkContext.broadcast(appTagsMap)

        val ansDf = appUsageDF
            .map { bt =>
                val tags = Try(appTagsBroadCast.value(bt.entityKey)).getOrElse(Seq())
                bt.imei1Md5 -> tags
            }
            .groupByKey(_._1)
            .mapGroups { case(imeiMd5, tags) =>
                val tagSeq = tags.toSeq
                val tagMap = tagSeq.flatMap(_._2)
                        .groupBy(s => s)
                        .mapValues(_.size)

                UserAppTags(imeiMd5, tagMap)
            }
            .filter(_.appTags.nonEmpty)

        ansDf
            .repartition(10)
            .write
            .mode(SaveMode.Overwrite)
            .parquet(args("output"))

        spark.stop()
    }
}
