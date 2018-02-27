package com.xiaomi.ad.keyword.marchinggiant.app

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import scopt.OptionParser

/**
  * Created by cailiming on 17-7-13.
  */
object AppInfoJoin {
    case class Config(appInput: String, similarityAppInput: String, keywordInput: String, queryInput: String, output: String)

    val configParser = new OptionParser[Config]("App Join") {
        override def showUsageOnError = true

        head("App Join", "1.0")

        opt[String]('a', "appInput") required() action {
            (a, cfg) => cfg.copy(appInput = a)
        } text "app input file path"

        opt[String]('s', "similarityAppInput") required() action {
            (s, cfg) => cfg.copy(similarityAppInput = s)
        } text "similarity app input file path"

        opt[String]('k', "keywordInput") required() action {
            (k, cfg) => cfg.copy(keywordInput = k)
        } text "keyword input file path"

        opt[String]('q', "queryInput") required() action {
            (q, cfg) => cfg.copy(queryInput = q)
        } text "query input file path"

        opt[String]('o', "output") required() action {
            (o, cfg) => cfg.copy(output = o)
        } text "output path"

        help("help") text "prints this usage text"
    }

    def main(args: Array[String]): Unit = {
        exec(args, new SparkConf())
    }

    def exec(args: Array[String], sparkConf: SparkConf) = {
        for(config <- configParser.parse(args, Config(null, null, null, null, null))) {
            val spark = SparkSession.builder().config(sparkConf).appName("Join App Info").getOrCreate()
            import spark.implicits._

            val similarityAppDf = spark.read.parquet(config.similarityAppInput)
                .as[SimilarityResult]
                .map(s => s.copy(apps = s.apps.filter(_.score >= 0.5)))
                .select($"packageName", $"apps".alias("similarApps"))

            val keywordDf = spark.read.parquet(config.keywordInput)
                .select($"pn".alias("packageName"), $"kw".alias("extractKeywords"))


            val queryDf = spark.read.parquet(config.queryInput)

            spark.read.parquet(config.appInput)
                .join(similarityAppDf, Seq("packageName"), "LEFT")
                .join(keywordDf, Seq("packageName"), "LEFT")
                .join(queryDf, Seq("appId"), "LEFT")
                .repartition(50)
                .write
                .mode(SaveMode.Overwrite)
                .format("parquet")
                .save(config.output)

            spark.stop()
        }
    }
}
