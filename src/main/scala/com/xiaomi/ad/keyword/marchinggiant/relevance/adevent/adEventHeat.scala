package com.xiaomi.ad.keyword.marchinggiant.relevance.adevent

import com.twitter.scalding.Args
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * create by liguoyu on 2018-05-09
  */

object adEventHeat {
  case class AdEvent(
                      servertime: Long,
                      eView: Int,
                      estartdownload: Int,
                      experimentid: Option[String],
                      masterimei: Option[String],
                      adId: Long,
                      appid: Long,
                      tagId: Option[String],
                      price: Long
                    )

  def main(mainArgs: Array[String]): Unit = {
    val args       = Args(mainArgs)
    val max        = args.getOrElse("thread", "5.5").toDouble
    val outputPath = args("output")
    val inputPaths = args("input")
    val conf = new SparkConf()
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc    = spark.sparkContext
    val hdfs  = org.apache.hadoop.fs.FileSystem.get(sc.hadoopConfiguration)

    import spark.implicits._

    val input = spark.read.parquet(inputPaths)
      .as[AdEvent]
      .rdd
      .filter(x => x.experimentid.isDefined && x.masterimei.isDefined && x.tagId.isDefined)
      .filter(x => x.tagId.get == "1.24.4.13" || x.tagId.get == "1.24.4.1")
    //.filter(x => x.tagId.get == "1.24.4.13" || x.tagId.get == "1.24.4.15" || x.tagId.get == "1.24.4.1")

    val expore = input
      .filter(x => x.eView == 1)
      .map { x =>
        x.appid -> 1
      }
      .reduceByKey(_ + _)

    val download = input
      .filter(x => x.estartdownload == 1)
      .map { x =>
        x.appid -> 1
      }
      .reduceByKey(_ + _)
      .filter(x => x._2 >= 20)

    val price = input
      .map(x => x.appid -> x.price)
      .groupByKey()
      .mapValues(x => (x.sum / x.size) / 100000.0)

    val join = expore
      .join(download)
      .join(price)
      .filter(x => x._2._1._1 > 0)
      .map {
        case (appId, ((e, d), p)) =>
          val cvr = d.toDouble / e
          (appId,  Math.log10(p)*cvr)
      }

    val count = join.count()

    val avgScore = join.map(_._2).sum()/count

    val output = join
      .map {
        case (appId, score) =>
          (appId, score/avgScore + 1)
      }
      .repartition(1)
      .sortBy(-_._2)
      .map {
        case (appId, score) =>
          var s = score
          if (s > max) {
            s = max
          }
          s"$appId\t$s"
      }

    output.saveAsTextFile(outputPath)
    spark.stop()

//    output.saveAsTextFile(outputPath)
  }

}
