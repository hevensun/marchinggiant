package com.xiaomi.ad.keyword.marchinggiant.query

import com.twitter.scalding.Args
import com.xiaomi.ad.keyword.marchinggiant.ner.BehaviorTag
import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.JavaConversions._

/**
  * Created by cailiming on 17-12-26.
  */
object SegQuery {
    def main(args: Array[String]): Unit = {
        val argv = Args(args)
        val sparkConf = new SparkConf()
        executeSingleWord(argv, sparkConf)
    }

    def execute(args: Args, sparkConf: SparkConf) = {
        val spark = SparkSession
            .builder()
            .config(sparkConf)
            .getOrCreate()

        import spark.implicits._

        val ans = spark.read.parquet(args("input"))
            .as[BehaviorTag]
            .filter(t => t.sourceId == 1 && t.text.trim.length >= 3)
            .map { bt =>
                ToAnalysis.parse(bt.text.trim)
                    .getTerms
                    .map { word =>
                        if(word.getNatureStr == "m")
                            processNum(word.getName)
                        else
                            word.getName
                    }
                    .mkString(" ")
            }

        ans.repartition(10)
            .write
            .mode(SaveMode.Overwrite)
            .text(args("output"))

        spark.stop()
    }

    def executeSingleWord(args: Args, sparkConf: SparkConf) = {
        val spark = SparkSession
            .builder()
            .config(sparkConf)
            .getOrCreate()

        import spark.implicits._

        val ans = spark.read.parquet(args("input"))
            .as[BehaviorTag]
            .filter(t => t.sourceId == 1 && t.text.trim.length >= 3)
            .map { bt =>
                val text = bt.text.trim
                val seged = ToAnalysis.parse(text)
                    .getTerms

                seged
                    .flatMap { term =>
                        if(term.getNatureStr == "en") {
                            Seq(term.getName)
                        } else if(term.getNatureStr == "m") {
                            Seq(processNum(term.getName))
                        } else {
                            term.getName.map(c => c + "")
                        }
                    }
                    .mkString(" ")
            }

        ans.repartition(10)
            .write
            .mode(SaveMode.Overwrite)
            .text(args("output"))

        spark.stop()
    }

    def processNum(input: String) = {
        val ans = new StringBuilder
        ans.append("0")
        input.foreach { c =>
            if((c >= '0' && c <= '9') || c == '.') {}
            else ans.append(c)
        }
        ans.toString
    }
}
