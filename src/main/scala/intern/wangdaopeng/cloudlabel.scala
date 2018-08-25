package com.xiaomi.intern.wangdaopeng

import com.twitter.scalding.Args
import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.xiaomi.ad.matrix.qu.wordnormalizer.WordNormalizer

import scala.collection.mutable.ArrayBuffer
object cloudlabel {

  ///用户对应的应用
  /**
    * |-- id: string (nullable = true)
    * |-- idType: byte (nullable = true)
    * |-- appUsage: map (nullable = true)
    * |    |-- key: string
    * |    |-- value: map (valueContainsNull = true)
    * |    |    |-- key: string
    * |    |    |-- value: long (valueContainsNull = true)
    */

  //  /user/h_miui_ad/matrix/qu//date=20180816
  /**
    * |-- packageName: string (nullable = true)
    * |-- displayName: string (nullable = true)
    * |-- category: array (nullable = true)
    * |    |-- element: struct (containsNull = true)
    * |    |    |-- catId: integer (nullable = true)
    * |    |    |-- catName: string (nullable = true)
    * |    |    |-- score: double (nullable = true)
    */

  /**need
    * top1 appName
    *
    *
    * top1-top4 appLabel
    *
    * top5-top7 appLabel
    *
    * top12-top20 appLabel
    *
    * top2-top3 appname
    */


  case class element(catId: Int, catName: String, Score: Double)
  case class appkeyInfo(packageName:String, displayName:String, category:Seq[element])


  def cutword(word:String): String ={
    val norword = WordNormalizer.normalize(word)
    if(word.length>=5){
      var tmp:String = word
      if (ToAnalysis.parse(word).getTerms().size()>=1)  tmp=ToAnalysis.parse(word).getTerms().get(0).getName
      return tmp
    }
    else  return word
  }

  //  def cutword(word:String): String ={
  //           return word
  //  }


  def main(args: Array[String]): Unit = {
    val argv = Args(args)
    execute(argv, new SparkConf())
  }
  def execute(args:Args ,sparkConf: SparkConf)= {
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()


    import spark.implicits._
    val appinfo = spark.read.parquet(args("appexpansion"))
      .as[appkeyInfo].filter(f => f.category!=null)
      .map(f => {
        val pName = f.packageName
        val dName = f.displayName
        val str = f.category.filter(e => e.catName !=null).map(e => e.catName).mkString("/")
        (pName, (dName, str))
      }).distinct().rdd

    val broaddict = spark.sparkContext.broadcast(appinfo.collectAsMap())

    //    appinfo.map(e => e._1+"\t"+e._2._1 + "\t"+e._2._2)
    //      .saveAsTextFile("/user/h_miui_ad/develop/wangdaopeng/appKeyInfo")




    //appusedaily
    val file1 = "/user/h_data_platform/platform/matrix/matrix_app_usage_daily/date=20180821/part-00196-92435610-59e4-4a10-9a43-828270fc31c2.snappy.parquet"
    val file2 = "/user/h_data_platform/platform/matrix/matrix_app_usage_daily/date=20180820/part-00099-bcea8303-a831-4a6d-b3f8-3b77715dbb73.snappy.parquet"
    import spark.implicits._
    val userinfo  =spark.read.parquet(args("userinfo"))
      .map{x=>
        val imei = x.getAs[String]("id")
        val userGender = x.getAs[Byte]("userGender")
        val userAge = x.getAs[Byte]("userAge")
        (imei,userGender,userAge)
      }
      .toDF("imei","userGender","userAge")


    val res = spark.read.parquet(args("appusedaily"))
      //    val res = spark.read.parquet(file1,file2)
      .flatMap(x=>{
      val id = x.getAs[String]("id")
      val imei_app_duration = x.getAs[Map[String, Map[String,Long]]]("appUsage")
        .map(e => {
          val app_packname = e._1
          val app_duration = e._2.getOrElse("usageDuration",0.toLong)
          (id, app_packname, app_duration)
        })
      imei_app_duration
    }).rdd.map(x=> ((x._1,x._2),x._3))
      .groupByKey()
      .map(x=>(x._1 ,x._2.toArray.sum))
      .map(x=>(x._1._1,(x._1._2,x._2)))
      .groupByKey().map(x=>{
      val imei =  x._1
      var top1appName = ""
      var top1appLabel= ""
      var top1_top4appLabel = ""
      var top5_top7appLabel = ""
      var top8_top11appLabel = ""
      var top12_top20appLabel = ""
      var top2_top3appName = ""
      val others =  new ArrayBuffer[String]()


      val appname_usetime = new ArrayBuffer[(String,Long)]()
      for(item<- x._2) appname_usetime.append((item._1,item._2))
      val app_usetime_sorted =  appname_usetime.sortBy(_._2).reverse

      for(i<-0.to(math.min(19,app_usetime_sorted.length-1))){

        if (broaddict.value.contains(app_usetime_sorted(i)._1))  others.append(broaddict.value(app_usetime_sorted(i)._1)._1)
        val tmpapplabel =  if(broaddict.value.contains(app_usetime_sorted(i)._1))  broaddict.value(app_usetime_sorted(i)._1)._2 else "None"
        if(tmpapplabel.split("/").length >=3){
          others.append(cutword(tmpapplabel.split("/")(1)))
          others.append(cutword(tmpapplabel.split("/")(2)))
        }
        else if  (tmpapplabel.split("/").length >=2){
          others.append(tmpapplabel.split("/")(1))
        }
      }
      val otherlabel:String = others.mkString("#")


      if(app_usetime_sorted.length>0) {
        top1appName = if (broaddict.value.contains(app_usetime_sorted(0)._1)) broaddict.value(app_usetime_sorted(0)._1)._1 else "Non"
        val tmpapplabel =  if(broaddict.value.contains(app_usetime_sorted(0)._1))  broaddict.value(app_usetime_sorted(0)._1)._2 else "None"
        if(tmpapplabel.contains("/") && tmpapplabel.split("/").length>=3 )
          top1appLabel  += cutword(tmpapplabel.split("/")(1)) +"#" +cutword(tmpapplabel.split("/")(2)+"#")
        else if(tmpapplabel.contains("/") && tmpapplabel.split("/").length>=2 )
          top1appLabel  += cutword(tmpapplabel.split("/")(1)) +"#"
        else
          top1appLabel  += cutword(tmpapplabel) + "#"
        //            top1appLabel =tmpapplabel
      }

      for(i<-0.to(math.min(3,app_usetime_sorted.length-1))){
        val tmpappname = app_usetime_sorted(i)._1
        val tmpapplabel =  if(broaddict.value.contains(tmpappname))  broaddict.value(tmpappname)._2 else "None"
        if(tmpapplabel.contains("/") && tmpapplabel.split("/").length>=3 )
          top1_top4appLabel  += cutword(tmpapplabel.split("/")(1)) +"#" +cutword(tmpapplabel.split("/")(2)+"#")
        else if(tmpapplabel.contains("/") && tmpapplabel.split("/").length>=2 )
          top1_top4appLabel  += cutword(tmpapplabel.split("/")(1)) +"#"
        else
          top1_top4appLabel  += cutword(tmpapplabel) + "#"

        //用于测试 app在字典中不存在则为None 负责为对应的label 观察其长度
        //            top1_top4appLabel +=tmpapplabel+"#"
      }

      for(i<-4.to(math.min(6,app_usetime_sorted.length-1))){
        val tmpappname = app_usetime_sorted(i)._1
        val tmpapplabel =  if(broaddict.value.contains(tmpappname))  broaddict.value(tmpappname)._2 else "None"
        if(tmpapplabel.contains("/") && tmpapplabel.split("/").length>=3 )
          top5_top7appLabel  += cutword(tmpapplabel.split("/")(1)) +"#" +cutword(tmpapplabel.split("/")(2))+"#"
        else if(tmpapplabel.contains("/") && tmpapplabel.split("/").length>=2)
          top5_top7appLabel  += cutword(tmpapplabel.split("/")(1))
        else top5_top7appLabel  +=cutword(tmpapplabel)+"#"
      }


      for (i <- 7.to(math.min(10,app_usetime_sorted.length-1))) {
        val tmpappname = app_usetime_sorted(i)._1
        val tmpapplabel =  if(broaddict.value.contains(tmpappname))  broaddict.value(tmpappname)._2 else "None"
        if(tmpapplabel.contains("/") && tmpapplabel.split("/").length>=3 )
          top8_top11appLabel += cutword(tmpapplabel.split("/")(1)) + "#" + cutword(tmpapplabel.split("/")(2))+ "#"
        else if (tmpapplabel.contains("/") && tmpapplabel.split("/").length>=2 )
          top8_top11appLabel += cutword(tmpapplabel.split("/")(1)) + "#"
        else top8_top11appLabel += cutword(tmpapplabel) + "#"
      }



      for (i <- 11.to(math.min(19,app_usetime_sorted.length-1))) {
        val tmpappname = app_usetime_sorted(i)._1
        val tmpapplabel =  if(broaddict.value.contains(tmpappname))  broaddict.value(tmpappname)._2 else "None"

        if (tmpapplabel.contains("/") && tmpapplabel.split("/").length>=3 )
          top12_top20appLabel += cutword(tmpapplabel.split("/")(1)) + "#" + cutword(tmpapplabel.split("/")(2)) + "#"
        else if (tmpapplabel.contains("/") && tmpapplabel.split("/").length>=2 )
          top12_top20appLabel += cutword(tmpapplabel.split("/")(1)) + "#"
        else top12_top20appLabel  += cutword(tmpapplabel)
      }


      if (app_usetime_sorted.length>=3) {
        val app2Ename = app_usetime_sorted(1)._1
        val app3Ename = app_usetime_sorted(2)._1
        val app2Cname = if(broaddict.value.contains(app2Ename))  broaddict.value(app2Ename)._1 else "None"
        val app3Cname = if(broaddict.value.contains(app3Ename))  broaddict.value(app3Ename)._1 else "None"
        top2_top3appName += app2Cname + "#" + app3Cname
      }

      /**modify here **/
      val applength = app_usetime_sorted.length

      ( imei,applength,top1appName,top1appLabel,top1_top4appLabel,top2_top3appName,top5_top7appLabel,top8_top11appLabel,top12_top20appLabel,otherlabel)
    })
      .toDF("imei","appnum","top1appName","top1appLabel","top1_top4appLabel","top2_top3appName","top5_top7appLabel","top8_top11appLabel","top12_top20appLabel","otherlabels")
      .join(userinfo,"imei")
      .write.mode(SaveMode.Overwrite).parquet(args("output"))
  }
}