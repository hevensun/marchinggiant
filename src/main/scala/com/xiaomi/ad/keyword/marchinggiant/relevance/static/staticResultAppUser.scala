package com.xiaomi.ad.keyword.marchinggiant.relevance.static

import com.twitter.scalding.Args
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.util.Try

/**
  * create by liguoyu 2018-05-08
  */

object staticResultAppUser {
  def main(args: Array[String]): Unit = {
    val input_params = Args(args)
    val conf = new SparkConf()
    excute(input_params, conf)
  }
  def excute(input_p:Args, spark_conf: SparkConf): Unit = {

    val spark = SparkSession.builder().config(spark_conf).getOrCreate()
    import spark.implicits._

    val appPath = input_p("appPath") //"/user/h_miui_ad/develop/liguoyu1/project/AppExtension/AppSimInput/appAdInfoCate_"+input_p("yestoday")
    val userPath = input_p("userPath") //"/user/h_miui_ad/develop/liguoyu1/project/AppExtension/UserInput/userCateInfo_"+input_p("yestoday")
    val resultPath = input_p("resultPath") //"/user/h_miui_ad/develop/liguoyu1/project/AppExtension/Result/re_sim_user2appid_"+input_p("yestoday")
    val appCount = spark.read.parquet(appPath).count()//getOrElse(0)
    val userCount = spark.read.parquet(userPath).count()//).getOrElse(0)
    val resultCount = spark.sparkContext.textFile(resultPath).count()//).getOrElse(0)
    if(appCount.intValue() <= 0||userCount.longValue() <= 10000L||resultCount.longValue() <= 10000L){
      val write_result = spark.sparkContext.makeRDD("生成结果有错误，请查阅日志！！！")
      write_result.saveAsTextFile(input_p("output"))
    }
    else{
      val write_result = spark.sparkContext.makeRDD(Seq("app numer :"+appCount.toString,
        " users category number :"+userCount.toString,
        "result count :"+resultCount.toString,
        "result path :"+resultPath))
      write_result.repartition(1).saveAsTextFile(input_p("output"))
    }
    spark.stop()
  }
}
