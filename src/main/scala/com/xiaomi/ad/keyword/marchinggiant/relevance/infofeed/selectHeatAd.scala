package com.xiaomi.ad.keyword.marchinggiant.relevance.infofeed

import com.twitter.scalding.Args
import org.apache.spark.SparkConf

/**
  * create by liguoyu on 2018-05-21
  * filter ad according to user click and download (:ad heats)
  */

object selectHeatAd {
  def main(args: Array[String]): Unit = {
    val input_param = Args(args)
    val conf = new SparkConf()
    excute(input_param, conf)
  }

  def excute(input_param: Args, conf: SparkConf): Unit = {

  }

}
