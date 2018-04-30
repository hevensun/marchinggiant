package com.xiaomi.ad.keyword.marchinggiant.app

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import scopt.OptionParser

import scala.util.Try

/**
  * Created by cailiming on 17-7-13.
  */
object CoClickApp {
    case class Config(input: String, output: String)

    val configParser = new OptionParser[Config]("App Category") {
        override def showUsageOnError = true

        head("App Category", "1.0")

        opt[String]('i', "input") required() action {
            (i, cfg) => cfg.copy(input = i)
        } text "query file path"

        opt[String]('o', "output") required() action {
            (o, cfg) => cfg.copy(output = o)
        } text "output path"

        help("help") text "prints this usage text"
    }

    def main(args: Array[String]): Unit = {
        execute(args, new SparkConf())
    }

    def execute(args: Array[String], sparkConf: SparkConf) = {
        for(config <- configParser.parse(args, Config(null, null))) {
            val spark = SparkSession.builder().appName("App Category").getOrCreate()
            import spark.implicits._

            val result = spark.read.parquet(config.input)
                .as[AppCoClick]
                .flatMap{ coc =>
                    if(coc.appIdCountSeq.size > 1500) {
                        Seq()
                    } else {
                        coc.appIdCountSeq
                            .take(20)
                            .combinations(2)
                            .map{ cs =>
                                val app1 = Try(cs.head.appId.toLong).getOrElse(0L)
                                val app2 = Try(cs(1).appId.toLong).getOrElse(0L)
                                val min = Math.min(app1, app2)
                                val max = Math.max(app1, app2)
                                AppPair(min, max, 1L)
                            }
                    }
                }
                .groupBy($"app1", $"app2")
                .agg(sum($"count").alias("count"))
                .orderBy($"count".desc)

            result
                .write
                .mode(SaveMode.Overwrite)
                .format("parquet")
                .save(s"${config.output}/parquet")

            result
                .map { r =>
                    val app1 = r.getAs[Long]("app1")
                    val app2 = r.getAs[Long]("app2")
                    val count = r.getAs[Long]("count")
                    s"$app1\t$app2\t$count"
                }
                .write
                .mode(SaveMode.Overwrite)
                .text(s"${config.output}/text")

            spark.stop()
        }
    }

}
