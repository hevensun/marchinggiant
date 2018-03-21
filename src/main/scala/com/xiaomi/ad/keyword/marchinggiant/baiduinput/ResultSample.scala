package com.xiaomi.ad.keyword.marchinggiant.baiduinput

import com.twitter.scalding.Args
import com.xiaomi.ad.keyword.marchinggiant.java.ACTrie
import com.xiaomi.ad.keyword.marchinggiant.ner.BehaviorTag
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.io.Source
import scala.collection.JavaConversions._

/**
  * Created by cailiming on 18-3-6.
  */
object ResultSample {
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

        val aCTrie = ACTrie.getInstance()

        Source.fromInputStream(ResultSample.getClass.getResourceAsStream("/dic-9w.txt"))
            .getLines()
            .foreach { l =>
                aCTrie.addKeyword(l)
            }

        val aCTrieBroadCast = spark.sparkContext.broadcast(aCTrie)

        val ansDf = spark.read.parquet(args("input"))
            .as[BehaviorTag]
            .filter { bt =>
                bt.sourceId == 4 && bt.extension("inputType") == "SEARCH"
            }
            .map { bt =>
                val text = bt.text
                val parsed = if (text.toLowerCase.length >= 20) ""
                else aCTrieBroadCast.value.parseTextAndMerge(text.toLowerCase).toSeq.mkString(" ")

                text + "\tSEG\t" + parsed
            }

        ansDf
            .repartition(10)
            .write
            .mode(SaveMode.Overwrite)
            .text(args("output"))

        spark.stop()
    }

}
