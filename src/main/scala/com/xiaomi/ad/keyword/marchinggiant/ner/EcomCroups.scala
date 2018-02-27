package com.xiaomi.ad.keyword.marchinggiant.ner

import com.twitter.scalding.Args
import com.xiaomi.ad.keyword.marchinggiant.java.ACTrie
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.io.Source
import scala.collection.JavaConversions._

/**
  * Created by cailiming on 17-10-30.
  */
object EcomCroups {
    def main(args: Array[String]): Unit = {
        val argv = Args(args)
        execute(argv, new SparkConf())
    }

    def execute(args: Args, sparkConf: SparkConf) = {
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._

        val words = Source.fromInputStream(EcomCroups.getClass.getResourceAsStream("/category-name-suning.txt"))
            .getLines()
            .map(_.toLowerCase)
            .toSeq

        val acTree = ACTrie.getInstance()

        words
            .foreach(acTree.addKeyword)

        val acTrieBroadCast = spark.sparkContext.broadcast(acTree)

        val ans = spark.read.parquet(args("input"))
            .as[BehaviorTag]
            .filter { b =>
                Seq(1, 4, 7).contains(b.sourceId) && b.text.length <= 20
            }
            .dropDuplicates("text")
            .map { bt =>
                val aCTrieIn = acTrieBroadCast.value

                val tagBuilder = new StringBuilder
                bt.text.indices.foreach(_ => tagBuilder.append('O'))

                val parsed = aCTrieIn.parseText(bt.text)

                val filtered = parsed
                    .filter { e =>
                        parsed.exists(ce => ce.getStart > e.getStart && ce.getEnd <= e.getEnd)
                    }

                filtered
                    .foreach { e =>
                        if(e.getStart == e.getEnd) {
                            tagBuilder(e.getStart) = 'S'
                        } else {
                            tagBuilder(e.getStart) = 'B'
                            ((e.getStart + 1) until e.getEnd)
                                .foreach(i => tagBuilder(i) = 'I')
                            tagBuilder(e.getEnd) = 'E'
                        }
                    }

                bt.text -> tagBuilder.toString()
            }
            .filter { a =>
                (a._2.contains("B") || a._2.contains("S")) && a._1.length == a._2.length
            }
            .map { a =>
                a._1 + "\t" + a._2
            }

        ans.repartition(1)
            .write
            .mode(SaveMode.Overwrite)
            .text(args("output"))

        spark.stop()
    }
}
