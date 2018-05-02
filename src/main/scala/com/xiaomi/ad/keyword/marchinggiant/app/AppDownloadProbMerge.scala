package com.xiaomi.ad.keyword.marchinggiant.app

import com.twitter.scalding.Args
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SaveMode, SparkSession}

/**
  * Created by cailiming on 18-2-26.
  */
object AppDownloadProbMerge {
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

        processAndSave(spark, args("input") + "/app2app", args("output") + "/app2app", 1)
        processAndSave(spark, args("input") + "/app2category", args("output") + "/app2category", 1)
        processAndSave(spark, args("input") + "/category2app", args("output") + "/category2app", 1)
        processAndSave(spark, args("input") + "/category2category", args("output") + "/category2category", 1)
        processAndSave(spark, args("input") + "/lv1category2lv1category", args("output") + "/lv1category2lv1category", 1)

        spark.stop()
    }

    def processAndSave(spark: SparkSession, inputPath: String, outputPath: String, num: Int) = {
        import spark.implicits._

        spark.read.text(inputPath).as[String]
            .map { line =>
                val split = line.split("\t", 3)
                val pval = Math.floor(split.last.toDouble * num).toInt

                s"${split.head}_${split(1)}\t$pval"
            }
            .repartition(1)
            .write
            .mode(SaveMode.Overwrite)
            .text(outputPath)
    }

    def execute1(args: Args, sparkConf: SparkConf) = {
        val spark = SparkSession
            .builder()
            .config(sparkConf)
            .getOrCreate()

        val app2app = readAndProcessed(spark, args("input") + "/app2app", "a2a", 1)
        val app2cate = readAndProcessed(spark, args("input") + "/app2category", "a2c", 100)
        val cate2app = readAndProcessed(spark, args("input") + "/category2app", "c2a", 100)
        val cate2cate = readAndProcessed(spark, args("input") + "/category2category", "c2c", 100)
        val lv1Cate2lv1Cate = readAndProcessed(spark, args("input") + "/lv1category2lv1category", "l1c2l1c", 100)

        val all = app2app ++ app2cate ++ cate2app ++ cate2cate ++ lv1Cate2lv1Cate

        spark.sparkContext.parallelize(all)
            .repartition(1)
            .saveAsTextFile(args("output"))

        spark.stop()
    }

    def readAndProcessed(spark: SparkSession, path: String, prefix: String, num: Int) = {
        import spark.implicits._

        val infoArr= spark.read.text(path).as[String]
            .map { line =>
                val split = line.split("\t", 3)
                val pval = Math.floor(split.last.toDouble * num).toInt
                s"${prefix}_${split.head}_${split(1)}\t$pval"
            }
            .rdd
            .collect()

        infoArr
    }
}
