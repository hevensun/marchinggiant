package com.xiaomi.ad.keyword.marchinggiant.appstore

import com.xiaomi.data.commons.spark.HdfsIO._
import com.xiaomi.data.spec.log.miuiads.AdLogV2
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import scopt.OptionParser

import scala.collection.JavaConversions._
import scala.util.Try

/**
  * Created by cailiming on 17-7-27.
  */
object AppQuery {
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

            spark.sparkContext.thriftSequenceFile(config.input, classOf[AdLogV2])
                .filter(a => a.tagId == "1.24.4.15")
                .map{ a =>
                    Try{
                        val query = a.requestParams("key_word")
                        val appIds = a.responseParams("app_ids")
                        AppQueryResponse(query, appIds)
                    }.getOrElse(AppQueryResponse("", ""))
                }
                .filter(a => a.query.nonEmpty && a.appIds.nonEmpty)
                .toDF()
                .dropDuplicates("query")
                .write
                .format("parquet")
                .mode(SaveMode.Overwrite)
                .save(config.output)

            spark.stop()
        }
    }
}
