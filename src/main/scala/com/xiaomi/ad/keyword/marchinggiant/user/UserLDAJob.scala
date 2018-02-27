package com.xiaomi.ad.keyword.marchinggiant.user

import com.twitter.scalding.Args
import com.xiaomi.ad.keyword.marchinggiant.ner.BehaviorTag
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.io.Source
import scala.util.Try

/**
  * Created by cailiming on 18-1-30.
  */
object UserLDAJob {
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

        val outAppBroadCast = spark.sparkContext.broadcast(
            Source.fromInputStream(UserCategoryJob.getClass.getResourceAsStream("/app-out.txt"))
                .getLines()
                .toSet
        )

        val appUsageDF = spark.read.parquet(args("appUsage"))
            .as[BehaviorTag]
            .filter(bt => !outAppBroadCast.value.contains(bt.entityKey))

        val behaviorDF = spark.read.parquet(args("behavior"))
            .as[BehaviorTag]

        appUsageDF.union(behaviorDF)
            .filter { bt =>
                bt.extension != null && (bt.extension.contains("6") || bt.extension.contains("3") || bt.extension.contains("2"))
            }
            .map { bt =>
                val emiCategories = bt.extension.getOrElse("6", "")
                    .split(" ")
                    .filter(_.nonEmpty)
                    .map { curC =>
                        val curSplit = curC.split(":", 2)
                        curSplit.head -> curSplit.last.toDouble
                    }
                    .filter(_._2 > 0.4)
                    .toSeq
                    .map(_._1 -> 1)

                val googleCategories = bt.extension.getOrElse("2", "")
                    .split(" ")
                    .filter(_.nonEmpty)
                    .map { curC =>
                        val curSplit = curC.split(":", 2)
                        curSplit.head -> curSplit.last.toDouble
                    }
                    .filter(_._2 > 0.4)
                    .toSeq
                    .map(_._1 -> 1)

                val topics = bt.extension.getOrElse("3", "")
                    .split(" ")
                    .filter(_.nonEmpty)
                    .map { curC =>
                        val curSplit = curC.split(":", 2)
                        curSplit.head -> curSplit.last.toDouble
                    }
                    .filter(_._2 > 0.4)
                    .toSeq
                    .map(_._1 -> 1)

                UserBehaviorsCate(bt.imei1Md5, emiCategories ++ googleCategories ++ topics)
            }
            .groupByKey(_.imeiMd5)
            .mapGroups { case(imeiMd5, ucs) =>
                val ucSeq = ucs.toSeq
                val merged = ucSeq.flatMap(_.behaviors).groupBy(_._1).mapValues(_.map(_._2).sum).toSeq

                UserBehaviorsCate(imeiMd5, merged)
            }
            .repartition(10)
            .write
            .mode(SaveMode.Overwrite)
            .format("parquet")
            .save(args("output"))

        spark.stop()
    }
}
