package com.xiaomi.ad.keyword.marchinggiant.app

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.json4s._
import org.json4s.jackson.JsonMethods._

/**
  * Created by cailiming on 17-7-26.
  */
object BaiduAppParser {
    implicit val formats = DefaultFormats

    def execute(args: Array[String]): Unit = {
        val spark = SparkSession.builder().master("local[3]").getOrCreate()
        import spark.implicits._

        spark
            .read
            .text("/home/mi/Develop/data/app-meta-info/baidu/app_info.txt")
            .as[String]
            .map{ line =>
                val info = parse(line)
                    .extract[BaiduApp]

                EnhancedAppInfo(info.packageName, 0L, 0L, info.displayName, info.level1CategoryName, info.level2CategoryName, null, info.introduction.mkString(" "), info.brief, Seq(), Seq(), Seq(), 3)
            }
            .write
            .format("parquet")
            .mode(SaveMode.Overwrite)
            .save("/home/mi/Develop/data/app-meta-info/baidu/parquet")

        spark.stop()
    }
}
