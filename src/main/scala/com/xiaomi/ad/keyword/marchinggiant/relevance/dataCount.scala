package com.xiaomi.ad.keyword.marchinggiant.relevance

import com.twitter.scalding.Args
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object dataCount {
  def main(args: Array[String]): Unit = {
    val paramArgs = Args(args)
    val conf = new SparkConf()
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._


//    getOrCreate
  }

}
