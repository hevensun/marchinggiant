package com.xiaomi.intern.wangdaopeng

import com.twitter.scalding.Args
import com.xiaomi.ad.keyword.marchinggiant.relevance.infofeed.userCategoryOptimize.execute
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

object task1 {

  ///用户对应的应用
  //  user/h_data_platform/platform/matrix/matrix_app_usage_daily
  /**
    * |-- id: string (nullable = true)
    * |-- idType: byte (nullable = true)
    * |-- appUsage: map (nullable = true)
    * |    |-- key: string
    * |    |-- value: map (valueContainsNull = true)
    * |    |    |-- key: string
    * |    |    |-- value: long (valueContainsNull = true)
    */
  /**
    * struct MatrixAppUsageDaily {
    * 1:optional string id;//用户id
    * 2:optional byte idType;//id类型
    * 3:optional map<string,map<string,i64>> appUsage;
    * }
    *
    */
  //  /user/h_miui_ad/matrix/qu/app-expansion/date=20180816
  /**
    *
    * |-- packageName: string (nullable = true)
    * |-- appId: long (nullable = true)
    * |-- apkId: long (nullable = true)
    * |-- displayName: string (nullable = true)
    * |-- level1CategoryName: string (nullable = true)
    * |-- level2CategoryName: string (nullable = true)
    * |-- publisherName: string (nullable = true)
    * |-- introduction: string (nullable = true)
    * |-- brief: string (nullable = true)
    * |-- keywords: array (nullable = true)
    * |    |-- element: string (containsNull = true)
    * |-- category: array (nullable = true)
    * |    |-- element: struct (containsNull = true)
    * |    |    |-- catId: integer (nullable = true)
    * |    |    |-- catName: string (nullable = true)
    * |    |    |-- score: double (nullable = true)
    * |-- emiCategory: array (nullable = true)
    * |    |-- element: struct (containsNull = true)
    * |    |    |-- name: string (nullable = true)
    * |    |    |-- score: double (nullable = true)
    * |-- lda: array (nullable = true)
    * |    |-- element: struct (containsNull = true)
    * |    |    |-- topicId: integer (nullable = true)
    * |    |    |-- topicName: string (nullable = true)
    * |    |    |-- score: double (nullable = true)
    * |-- source: integer (nullable = true)
    * |-- date: integer (nullable = true)
    * |-- similarApps: array (nullable = true)
    * |    |-- element: struct (containsNull = true)
    * |    |    |-- packageName: string (nullable = true)
    * |    |    |-- displayName: string (nullable = true)
    * |    |    |-- score: double (nullable = true)
    * |-- storeQueries: array (nullable = true)
    * |    |-- element: struct (containsNull = true)
    * |    |    |-- query: string (nullable = true)
    * |    |    |-- recordNum: integer (nullable = true)
    * |    |    |-- imeiNum: integer (nullable = true)
    * |-- browserQueries: array (nullable = true)
    * |    |-- element: struct (containsNull = true)
    * |    |    |-- query: string (nullable = true)
    * |    |    |-- score: double (nullable = true)
    * |-- specialWords: array (nullable = true)
    * |    |-- element: struct (containsNull = true)
    * |    |    |-- word: string (nullable = true)
    * |    |    |-- score: double (nullable = true)
    *
    *
    */
  //appinfo

  case class element(catId: Int, catName: String, Score: Double)
  case class appkeyInfo(packageName:String, displayName:String, category:Seq[element])


  def main(args: Array[String]): Unit = {
    val argv = Args(args)
    execute(argv, new SparkConf())
  }
  def execute(args:Args ,sparkConf: SparkConf)= {
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()


    import spark.implicits._
    val appinfo = spark.read.parquet("/user/h_miui_ad/matrix/qu/app-expansion/date=20180816")
      .as[appkeyInfo].filter(f => f.category!=null)
      .map(f => {
        val pName = f.packageName
        val dName = f.displayName
        val str = f.category.filter(e => e.catName !=null).map(e => e.catName).mkString(";")
        (pName, (dName, str))
      }).distinct().rdd


    val broaddict = spark.sparkContext.broadcast(appinfo.collectAsMap())

    appinfo.map(e => e._1+"\t"+e._2._1 + "\t"+e._2._2)
      .saveAsTextFile("/user/h_miui_ad/develop/wangdaopeng/appKeyInfo0816")


    import spark.implicits._
    val userInfo = spark.read.parquet("/user/h_data_platform/platform/matrix/matrix_app_usage_daily/date=20180816")
      //        .as[MatrixAppUsageDaily]
      .map { x =>
      val imei = x.getAs[String]("id")
      val app_usage = x.getAs[Map[String, Map[String, Long]]]("appUsage")
      val appName = if(app_usage.nonEmpty) app_usage.map(r=> {
        if(broaddict.value.contains(r._1)) {
          broaddict.value(r._1)._1
        }
      }) else  Seq()

      val appLabel = if(app_usage.nonEmpty) app_usage.map(r=> {
        if(broaddict.value.contains(r._1)) {
          broaddict.value(r._1)._2
        }
      }) else  Seq()
      ///user/h_miui_ad/develop/wangdaopeng/cossilarity/userCateInfo_20180819/part-00462-d306ef88-8a09-4897-967c-ccb32fe291d5.snappy.parquet
      (imei, appName.mkString("###"), appLabel.mkString("###"))

    }.toDF("imei","appname","applabel")

    userInfo.rdd.map(e => e.getAs[String]("imei")+"\t"+e.getAs[String]("appname")+"\t"+e.getAs[String]("applabel")).saveAsTextFile("/user/h_miui_ad/develop/wangdaopeng/userAppNameCate0816")

    val infofeed  = spark.read.parquet("")
      .map{x=>
        val id =  x.getAs[String]("id")
        val text = x.getAs[String]("text")
        (id,text)
      }.toDF("imei","infofeed")


    val ss = spark.read.parquet("/user/h_miui_ad/develop/liguoyu1/cosSimilarity/userCateInfo_20180820").map(x=>
    {
      val imei = x.getAs[String]("imei1Md5")
      val label= x.getAs[Array[String]]("kwSeq") ++ x.getAs[Array[String]]("gCatSeq")
      (imei,label)
    }
    ).toDF("imei","label").join(userInfo,"imei").join(infofeed,"imei").toDF().write.mode(SaveMode.Overwrite).parquet("/user/h_miui_ad/develop/wangdaopeng/ress0819");

  }
}