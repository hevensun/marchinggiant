package com.xiaomi.ad.keyword.marchinggiant.baiduinput

import com.twitter.scalding.Args
import com.xiaomi.ad.keyword.marchinggiant.ner.BehaviorTag
import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.JavaConversions._

/**
  * Created by cailiming on 18-3-5.
  */
object BaiduInputStatistics {
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

        val ansDf = spark.read.parquet(args("input"))
            .as[BehaviorTag]
            .filter(_.sourceId == 4)
            .flatMap { bt =>
                ToAnalysis.parseAndRmStopWords(bt.text)
                    .getTerms
                    .filter { term =>
                        term.getNatureStr != "null" && (term.getNatureStr.startsWith("n") || term.getNatureStr.startsWith("v"))
                    }
                    .map(_.getName -> 1)
            }
            .groupByKey(_._1)
            .mapGroups { case(key, vs) =>
                key + "\t" + vs.size
            }

        ansDf
            .repartition(1)
            .write
            .mode(SaveMode.Overwrite)
            .text(args("output"))

        spark.stop()
    }
}
