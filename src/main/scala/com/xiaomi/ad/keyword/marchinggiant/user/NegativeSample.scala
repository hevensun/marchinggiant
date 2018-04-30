package com.xiaomi.ad.keyword.marchinggiant.user

import com.twitter.scalding.Args
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.util.Random

/**
  * Created by cailiming on 18-1-18.
  */
object NegativeSample {
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

        spark.read.text(args("input"))
            .as[String]
            .filter { line =>
                val random = Random.nextDouble()
                line.split("\t").head.trim == "1" || random <= 0.05
            }
            .repartition(10)
            .write
            .mode(SaveMode.Overwrite)
            .option("compression", "gzip")
            .text(args("output"))

        spark.stop()
    }
}
