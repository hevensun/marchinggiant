//package intern.wangdaopeng
//
//package com.xiaomi.intern.wangdaopeng
//
//package com.xiaomi.intern.wangdaopeng
//
//import com.twitter.scalding.Args
//import org.ansj.splitWord.analysis.ToAnalysis
//import org.apache.spark.SparkConf
//import org.apache.spark.sql.{SaveMode, SparkSession}
//
//import scala.collection.mutable.ArrayBuffer
//object test1 {
//
//
//  def main(args: Array[String]): Unit = {
//    ///user/h_miui_ad/matrix/qu/app-expansion/date=20180823
//    val s = new ArrayBuffer[(String, Int)]()
//    for(i<-1.to(10))  s.append(("a",i))
//    val b =s.sortBy(_._2).reverse
//    for(i<-b) println(i)
//    //    ss.filter(x=>x.getAs[String]("displayName")=="微信").select("category")
    val spark =SparkSession.builder().config(new SparkConf()).getOrCreate()

//  }
//}