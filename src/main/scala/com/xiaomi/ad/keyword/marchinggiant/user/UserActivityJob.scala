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
object UserActivityJob {
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

        val adEventDF = spark.read.text(args("adEvent")).as[String]
            .map { line =>
                val split = line.split("\t", 27)
                AdEvent(split.head, split(12).toLong)
            }

        val appUsageDF = spark.read.parquet(args("appUsage"))
            .as[BehaviorTag]

        val behaviorDF = spark.read.parquet(args("behavior"))
            .as[BehaviorTag]

        appUsageDF.union(behaviorDF)

        spark.stop()
    }
}
