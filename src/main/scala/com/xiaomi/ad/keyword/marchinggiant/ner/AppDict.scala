package com.xiaomi.ad.keyword.marchinggiant.ner

import com.twitter.scalding.Args
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by cailiming on 17-11-1.
  */
object AppDict {
    def main(args: Array[String]): Unit = {
        val argv = Args(args)
        execute(argv, new SparkConf())
    }

    def execute(args: Args, sparkConf: SparkConf) = {
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._

        val appPV = spark.read.text(args("appPV"))
            .as[String]
            .map { line =>
                val split = line.split("\t")
                split.head -> split.last.toInt
            }
            .collect()
            .toMap

        val appPVBroadCast = spark.sparkContext.broadcast(appPV)

        spark.read.parquet(args("input"))
            .select($"packageName", $"displayName")
            .filter { row =>
                val displayName = row.getAs[String]("displayName")
                displayName.nonEmpty && !displayName.map(c => c >= 'a' && c <= 'z').reduce(_ && _) && displayName.map(c => (c >= 0x4e00 && c <= 0x9fa5) || (c >= '0' && c <= '9')).reduce(_ && _)
            }
            .map { row =>
                val packageName = row.getAs[String]("packageName")
                val displayName = row.getAs[String]("displayName")
                (displayName.split("-|\\s|\\(|:").head.trim, appPVBroadCast.value.getOrElse(packageName, 0))
            }
            .groupByKey(_._1)
            .mapGroups { case(k, vs) =>
                val maxV = vs.map(_._2).max
                k.trim + "\t" + maxV
            }
            .repartition(1)
            .write
            .mode(SaveMode.Overwrite)
            .text(args("output"))

        spark.stop()
    }
}
