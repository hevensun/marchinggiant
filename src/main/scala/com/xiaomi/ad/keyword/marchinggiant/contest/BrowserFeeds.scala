package com.xiaomi.ad.keyword.marchinggiant.contest

import java.io.{BufferedWriter, FileWriter, OutputStreamWriter}
import java.util.Date

import com.twitter.scalding.{Args, RichDate}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.joda.time.DateTime

/**
  * Created by cailiming on 17-9-7.
  */
object BrowserFeeds {
    def main(args: Array[String]): Unit = {
        val argv = Args(args)
        execute(argv, new SparkConf())
    }

    def execute(args: Args, sparkConf: SparkConf) = {
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._

        val imeiDate = args("imeiDate")

        val imei = spark.read.text(args("imeiPath") + s"/date={$imeiDate}")
            .as[String]
            .map{ l =>
                val splits = l.split("\t")
                splits.head.trim
            }
            .distinct()
            .select($"value".alias("imei1Md5"))

        val feedDate = args("feedDate").split(",")
        var i = 1

        feedDate
            .foreach{ curDate =>
//                val fs = FileSystem.get(new Configuration())
//                val dateStr = if(i < 10) "0" + i else i + ""
//                val path = new Path(args("output") + s"/$dateStr")
//                val bw = new BufferedWriter(new OutputStreamWriter(fs.create(path), "UTF-8"))

                spark.read.parquet(args("feedPath") + s"/date=$curDate")
                    .filter($"sourceId" === 2)
                    .join(imei, Seq("imei1Md5"))
                    .map{ r =>
                        val imei = r.getAs[String]("imei1Md5")
                        val text = r.getAs[String]("text").replace("\t", "")
                        val timeStamp = r.getAs[Long]("timeStamp")

                        val hm = new DateTime(timeStamp).toString("HHmm")
                        val time = if(i < 10) "0" + i + hm else i + "" + hm
                        imei + "\t" + text + "\t" + time
                    }
                    .repartition(1)
                    .write
                    .mode(SaveMode.Overwrite)
                    .text(args("output") + s"/date=$curDate")

                i = i + 1
            }

        spark.stop()
    }
}
