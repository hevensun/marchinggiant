package com.xiaomi.ad.keyword.marchinggiant.user

import com.twitter.scalding.Args
import com.xiaomi.ad.keyword.marchinggiant.ner.BehaviorTag
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.util.Try

/**
  * Created by cailiming on 18-1-31.
  */
object UserAppEmiCategory {
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
                        category -> 1
                    }
                    .toSeq

                UserEmiCategories(bt.imei1Md5, categories)
            }
            .groupByKey(_.imeiMd5)
            .mapGroups { case(imeiMd5, ucs) =>
                val ucSeq = ucs.toSeq
                val cates = ucSeq.flatMap(_.categories).groupBy(_._1).mapValues(_.map(_._2).sum).toSeq

                UserEmiCategories(imeiMd5, cates)
            }
            .repartition(10)
            .write
            .mode(SaveMode.Overwrite)
            .format("parquet")
            .save(args("output"))

        spark.stop()
    }
}
