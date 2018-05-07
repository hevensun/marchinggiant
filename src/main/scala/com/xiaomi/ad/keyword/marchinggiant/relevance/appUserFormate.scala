package com.xiaomi.ad.keyword.marchinggiant.relevance

import com.twitter.scalding.Args
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * crteate by liguoyu on 2018-05-07
  * format app user similarly
  */

object appUserFormate {
  case class cosResult1(appId: String, cosSim: Double)

  case class termInfo(imei: String, cosSimG: Seq[cosResult1], cosSimE: Seq[cosResult1], cosSimL: Seq[cosResult1],appCosSimG: Seq[cosResult1], appCosSimE: Seq[cosResult1], appCosSimL: Seq[cosResult1])


  def main(args: Array[String]): Unit = {
    val argv = Args(args)
    execute(argv, new SparkConf())
  }

  def execute(args: Args, sparkConf: SparkConf) = {
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    spark.read.parquet(args("input"))
      .as[termInfo]
      .map { m =>
        val imeiStr = m.imei
        val gCateList = m.cosSimG
        val appExMap = m.appCosSimG.map(r=>(r.appId, r.cosSim))toMap
        val simcaMap = m.cosSimG.map(r=>(r.appId, r.cosSim)).toMap
        val unionList = appExMap.keySet.union(simcaMap.keySet)
        val simApps = unionList.map{r=>
          val score1 = simcaMap.getOrElse(r, 0.0) + 1.0
          val score2 = appExMap.getOrElse(r, 0.0) + 1.0
          f"${r}%s:${score1*score2}%2.4f"
        }.mkString(";")

        if (simApps.nonEmpty) {
          s"$imeiStr,$simApps"
        } else {
          ""
        }
      }
      .filter(_.nonEmpty)
      .repartition(100)
      .write
      .mode(SaveMode.Overwrite)
      .text(args("output"))
    spark.stop()
    println("task finished!")
  }
}
