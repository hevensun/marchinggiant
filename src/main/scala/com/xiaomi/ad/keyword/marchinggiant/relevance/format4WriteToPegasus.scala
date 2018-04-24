package com.xiaomi.ad.keyword.marchinggiant.relevance

import com.twitter.scalding.Args
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object format4WriteToPegasus {

    case class cosResult1(appId: String, cosSim: Double)

    case class termInfo(imei: String, cosSimG: Seq[cosResult1], cosSimE: Seq[cosResult1], cosSimL: Seq[cosResult1])


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
                val simApps = gCateList.map {
                    t =>
                        val score1 = t.cosSim + 1.0
                        f"${t.appId}%s:$score1%2.4f"
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
    }
}
