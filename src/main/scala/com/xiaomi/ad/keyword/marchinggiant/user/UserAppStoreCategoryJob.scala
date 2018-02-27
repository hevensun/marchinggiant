package com.xiaomi.ad.keyword.marchinggiant.user

import com.twitter.scalding.Args
import com.xiaomi.ad.keyword.marchinggiant.ner.BehaviorTag
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by cailiming on 18-1-31.
  */
object UserAppStoreCategoryJob {
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

        val matrixDf = spark.read.parquet(args("matrix"))
            .as[BehaviorTag]
            .filter { bt =>
                (bt.sourceId == 3 || bt.sourceId == 5) && bt.extension.contains("2")
            }
            .groupByKey(_.imei1Md5)
            .mapGroups { case (imeiMd5, gps) =>
                val categories = gps.map { bt =>
                    val cates = bt.extension.getOrElse("2", "")
                        .split(" ")
                        .map(c => c.split(":").head)
                        .toSeq
                    bt.timeStamp -> cates
                }
                .toMap

                UserGoogleCategories(imeiMd5, categories)
            }
            .filter(_.categories.nonEmpty)

    }
}
