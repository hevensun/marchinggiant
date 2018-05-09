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

    val appInfoPath = "/user/h_miui_ad/develop/liguoyu1/adinfoSimInput/appAdInfoCate_"+input_p("yestoday")
    val userPath = "/user/h_miui_ad/develop/liguoyu1/cosSimilarity/userCateInfo_"+input_p("yestoday")
    val resultPath = "/user/h_miui_ad/develop/liguoyu1/project/matrix/data/relevance/infofeed/re_sim_user2appid_"+input_p("yestoday")
    val appCount = spark.read.parquet(appInfoPath).count()//getOrElse(0)
    val userCount = spark.read.parquet(userPath).count()//).getOrElse(0)
    val resultCount = spark.sparkContext.textFile(resultPath).count()//).getOrElse(0)
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
