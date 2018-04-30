package com.xiaomi.ad.keyword.marchinggiant.appstore

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import scopt.OptionParser
import org.apache.spark.sql.functions._

/**
  * Created by cailiming on 17-7-28.
  */
object AppCoDownload {
    case class Config(input: String, output: String)

    val configParser = new OptionParser[Config]("App Similarity") {
        override def showUsageOnError = true

        head("App Similarity", "1.0")

        opt[String]('i', "input") required() action {
            (i, cfg) => cfg.copy(input = i)
        } text "App Input Path."

        opt[String]('o', "output") required() action {
            (o, cfg) => cfg.copy(output = o)
        } text "Result Output Path."

        help("help") text "prints this usage text"
    }

    def main(args: Array[String]): Unit = {
        execute(args, new SparkConf())
    }

    def execute(args: Array[String], sparkConf: SparkConf): Unit = {
        for (config <- configParser.parse(args, Config(null, null))) {
            val spark = SparkSession.builder().config(sparkConf).getOrCreate()
            import spark.implicits._

            val chineseUdf = udf { query: String =>
                query != null && query.nonEmpty && query.length > 1 && query.length < 10 && query.map(t => t >= 0x4e00 && t <= 0x9fbb).reduce(_ && _)
            }

            val result = spark.read.parquet(config.input)
                .select($"imei", $"k".alias("query"), $"appId")
                .filter(chineseUdf($"query"))
                .groupBy($"imei", $"query", $"appId")
                .count()
                .select($"query", $"appId", $"count".alias("pv"), lit(1).alias("uv"))
                .groupBy($"query", $"appId")
                .agg(sum("pv").alias("pv"), sum("uv").alias("uv"))
                .as[QueryAppPVUV]
                .groupByKey(_.query)
                .mapGroups{ case(query, apps) =>
                    val appSeq = apps.toSeq

                    val downloads = if(appSeq.size > 1000) {
                        Seq()
                    } else {
                        appSeq
                            .map{ q =>
                                AppDownload(q.appId, q.pv, q.uv.toInt)
                            }
                            .sortBy(-_.recordNum)
                    }

                    AppCoClick(query, downloads)
                }


            result.write.format("parquet").mode(SaveMode.Overwrite).save(config.output)

            spark.stop()
        }
    }

}
