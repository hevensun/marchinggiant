package com.xiaomi.ad.keyword.marchinggiant.user

import com.twitter.scalding.Args
import com.xiaomi.ad.keyword.marchinggiant.ner.BehaviorTag
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.io.Source
import scala.util.Try

/**
  * Created by cailiming on 18-1-18.
  */
object UserCategoryJob {
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

//        val outAppBroadCast = spark.sparkContext.broadcast(
//            Source.fromInputStream(UserCategoryJob.getClass.getResourceAsStream("/app-out.txt"))
//                .getLines()
//                .toSet
//        )

        val appUsageDF = spark.read.parquet(args("appUsage"))
            .as[BehaviorTag]
//            .filter(bt => !outAppBroadCast.value.contains(bt.entityKey))

//        val behaviorDF = spark.read.parquet(args("behavior"))
//            .as[BehaviorTag]

        appUsageDF
            .filter { bt =>
                bt.extension != null && bt.extension.contains("6")
            }
            .map { bt =>
                val categoryStr = bt.extension.getOrElse("6", "")
                val categories = categoryStr.split(" ")
                    .filter(_.nonEmpty)
                    .map { curC =>
                        val curSplit = curC.split(":", 2)
                        curSplit.head -> curSplit.last.toDouble
                    }
                    .filter(_._2 > 0.3)
                    .map { case(category, score) =>
                        val categorySplit = category.split("-")
                        (categorySplit.head, Try(categorySplit(1)).getOrElse(""))
                    }
                    .toSeq

                val lev1 = categories
                    .map(_._1).distinct.map(c => c -> 1)
                val lev2 = categories
                    .map(_._2).distinct.map(c => c -> 1)

                UserCategories(bt.imei1Md5, lev1, lev2)
            }
            .groupByKey(_.imeiMd5)
            .mapGroups { case(imeiMd5, ucs) =>
                val ucSeq = ucs.toSeq
                val lev1 = ucSeq.flatMap(_.lev1Categories).groupBy(_._1).mapValues(_.map(_._2).sum).toSeq
                val lev2 = ucSeq.flatMap(_.lev2Categories).groupBy(_._1).mapValues(_.map(_._2).sum).toSeq

                UserCategories(imeiMd5, lev1, lev2)
            }
            .repartition(10)
            .write
            .mode(SaveMode.Overwrite)
            .format("parquet")
            .save(args("output"))

        spark.stop()
    }
}
