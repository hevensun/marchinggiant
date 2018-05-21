package com.xiaomi.ad.keyword.marchinggiant.relevance.static

import com.twitter.scalding.Args
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.util.Try

object staticResultAdinfoUserSim {
  def main(args: Array[String]): Unit = {
    val input_params = Args(args)
    val conf = new SparkConf()
    excute(input_params, conf)
  }
  def excute(input_p:Args, spark_conf: SparkConf): Unit = {

    val spark = SparkSession.builder().config(spark_conf).getOrCreate()
    import spark.implicits._

    val appInfoPath = input_p("appInfoPath")
    val userPath = input_p("userPath")
    val resultPath = input_p("resultPath")
    val appCount = spark.read.parquet(appInfoPath).count()
    val userCount = spark.read.parquet(userPath).count()
    val resultCount = spark.sparkContext.textFile(resultPath).count()
    if(appCount.toInt <= 0||userCount.toLong <= 10000L||resultCount.toLong <= 10000L){
      val write_result = spark.sparkContext.makeRDD("生成结果有错误，请查阅日志！！！")
      write_result.saveAsTextFile(input_p("output"))
    }
    else{
      val write_result = spark.sparkContext.makeRDD(Seq("Ad numer :"+appCount.toString,
        " users category number :"+userCount.toString,
        "result count :"+resultCount.toString,
        "result path :"+resultPath))
      write_result.repartition(1).saveAsTextFile(input_p("output"))
    }
    spark.stop()
  }

}
