package com.xiaomi.ad.keyword.marchinggiant.user

import com.twitter.scalding.Args
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.util.Random

/**
  * Created by cailiming on 18-2-1.
  */
object AdEventSample {
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

        val inputDF = spark.read.text(args("input"))
            .as[String]

        val spamImei = inputDF
            .map { line =>
                val item = line.split("\t", 27)
                item(0) -> 1
            }
            .groupByKey(_._1)
            .mapGroups { case(k, vs) =>
                k -> vs.length
            }
            .filter(_._2 > 100)
            .map(_._1)
            .collect()
            .toSet

        val spamImeiBroadcast = spark.sparkContext.broadcast(spamImei)

        spark.read.text(args("input"))
            .as[String]
            .filter { line =>
                val split = line.split("\t")
                if(spamImeiBroadcast.value.contains(split.head))
                    false
                else if(split(1) == "1")
                    true
                else {
                    val random = Random.nextDouble()
                    random <= 0.05
                }
            }
            .repartition(2)
            .write
            .mode(SaveMode.Overwrite)
            .text(args("output"))

        spark.stop()
    }
}
