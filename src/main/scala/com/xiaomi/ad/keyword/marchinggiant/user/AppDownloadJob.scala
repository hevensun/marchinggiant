package com.xiaomi.ad.keyword.marchinggiant.user

import com.twitter.scalding.Args
import com.xiaomi.ad.keyword.marchinggiant.ner.BehaviorTag
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

/**
  * Created by cailiming on 18-1-31.
  */
object AppDownloadJob {
    val DUR1 = 10 * 60 * 1000
    val DUR2 = 30 * 60 * 1000
    val DUR3 = 60 * 60 * 1000
    val DUR4 = 2 * 60 * 60 * 1000
    val DUR5 = 6 * 60 * 60 * 1000
    val DUR6 = 24 * 60 * 60 * 1000

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

        val pairDF = spark.read.parquet(args("input"))
            .as[BehaviorTag]
            .filter { bt =>
                bt.sourceId == 3 && bt.actionId == 4
            }
            .groupByKey(_.imei1Md5)
            .mapGroups { case (imeiMd5, bts) =>
                val sortedDownloads = bts.map { cbt =>
                    cbt.timeStamp -> cbt.text
                }.toSeq.sortBy(_._1)

                val ansBuilder1 = scala.collection.mutable.MutableList.newBuilder[String]
                val ansBuilder2 = scala.collection.mutable.MutableList.newBuilder[String]
                val ansBuilder3 = scala.collection.mutable.MutableList.newBuilder[String]
                val ansBuilder4 = scala.collection.mutable.MutableList.newBuilder[String]
                val ansBuilder5 = scala.collection.mutable.MutableList.newBuilder[String]
                val ansBuilder6 = scala.collection.mutable.MutableList.newBuilder[String]

                sortedDownloads
                    .zipWithIndex
                    .foreach { case ((time, pn), index) =>
                        if (index > 0) {
                            val lastOne = sortedDownloads(index - 1)
                            val str = lastOne._2 + "\t" + pn
                            if (time - lastOne._1 <= DUR1) {
                                ansBuilder1 += str
                            } else if (time - lastOne._1 <= DUR2) {
                                ansBuilder2 += str
                            } else if (time - lastOne._1 <= DUR3) {
                                ansBuilder3 += str
                            } else if (time - lastOne._1 <= DUR4) {
                                ansBuilder4 += str
                            } else if (time - lastOne._1 <= DUR5) {
                                ansBuilder5 += str
                            } else if (time - lastOne._1 <= DUR6) {
                                ansBuilder6 += str
                            }
                        }
                    }

                AppDownloadBehaviors(ansBuilder1.result(), ansBuilder2.result(), ansBuilder3.result(), ansBuilder4.result(), ansBuilder5.result(), ansBuilder6.result())
            }.cache()

        groupAndSave(pairDF.flatMap(_.pair1), args("output") + "/dur1", spark)
        groupAndSave(pairDF.flatMap(_.pair2), args("output") + "/dur2", spark)
        groupAndSave(pairDF.flatMap(_.pair3), args("output") + "/dur3", spark)
        groupAndSave(pairDF.flatMap(_.pair4), args("output") + "/dur4", spark)
        groupAndSave(pairDF.flatMap(_.pair5), args("output") + "/dur5", spark)
        groupAndSave(pairDF.flatMap(_.pair6), args("output") + "/dur6", spark)

        pairDF.unpersist()

        spark.stop()
    }

    def groupAndSave(ds: Dataset[String], outPath: String, spark: SparkSession): Unit = {
        import spark.implicits._

        ds.groupBy($"value")
            .agg(count($"value").alias("cnt"))
            .map { row =>
                val key = row.getAs[String]("value")
                val cnt = row.getAs[Long]("cnt")

                key + "\t" + cnt
            }
            .repartition(2)
            .write
            .mode(SaveMode.Overwrite)
            .text(outPath)
    }
}
