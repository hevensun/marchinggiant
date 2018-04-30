package com.xiaomi.ad.keyword.marchinggiant.relevance

import com.twitter.scalding.Args
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object imeiDistribute {
  def main(args: Array[String]): Unit = {
    val argsParames = Args(args)
    val conf = new SparkConf()
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    val data = spark.read.parquet(argsParames("input")).toDF().map{r=>
      val imei = r.getAs[String]("imei1Md5")
      (imei, 1)
    }.groupByKey(f=>f._1)
      .mapGroups{case(key, value)=>
        val imei = key
        val size = value.toArray.size
        (imei, size)
      }.write.mode(SaveMode.Overwrite).parquet(argsParames("output"))

  }

}
