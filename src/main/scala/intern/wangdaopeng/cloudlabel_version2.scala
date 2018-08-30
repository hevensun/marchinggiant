package com.xiaomi.intern.wangdaopeng
import com.twitter.scalding.Args
import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.xiaomi.ad.matrix.qu.wordnormalizer.WordNormalizer

import scala.collection.mutable.ArrayBuffer

object cloudlabel_version2 {

  case class appkeyInfo(packageName:String, displayName:String, level1CategoryName:String ,level2CategoryName:String )
  val illegalAppset = Set(
    "com.miui.systemAdSolution",
    "com.miui.home",
    "com.android.contacts",
    "com.android.mms",
    "com.miui.gallery",
    "com.android.settings",
    "com.android.camera",
    "com.miui.securitycenter",
    "com.android.deskclock",
    "com.miui.weather2",
    "com.android.phone",
    "com.android.quicksearchbox",
    "com.miui.cleanmaster",
    "com.xiaomi.market",
    "com.android.calendar",
    "com.android.fileexplorer",
    "com.miui.calculator",
    "com.android.thememanager",
    "com.miui.antispam",
    "com.miui.notes",
    "com.miui.yellowpage",
    "com.miui.videoplayer",
    "com.xiaomi.gamecenter",
    "com.xiaomi.account",
    "com.android.providers.downloads.ui",
    "com.android.updater",
    "com.xiaomi.scanner",
    "com.android.calculator2",
    "com.android.soundrecorder",
    "com.mipay.wallet",
    "com.miui.securityadd",
    "com.miui.personalassistant",
    "com.miui.securitycore",
    "com.lbe.security.miui",
    "com.miui.systemAdSolution",
    "com.miui.compass",
    "com.xiaomi.vip",
    "com.miui.powerkeeper",
    "com.miui.voiceassist",
    "com.miui.voip",
    "com.miui.fm",
    "com.xiaomi.mipicks",
    "com.android.systemui",
    "com.miui.bugreport",
    "com.miui.virtualsim",
    "com.miui.networkassistant",
    "com.sohu.inputmethod.sogou.xiaomi",
    "com.miui.mipub",
    "com.miui.backup",
    "com.miui.touchassistant",
    "com.xiaomi.jr",
    "com.xiaomi.miplay",
    "com.xiaomi.simactivate.service",
    "com.miui.cloudbackup",
    "com.xiaomi.finddevice",
    "com.xiaomi.payment",
    "com.xiaomi.midrop",
    "com.google.android.gsf.login",
    "com.android.nfc",
    "com.xiaomi.discover",
    "com.miui.system",
    "com.miui.barcodescanner",
    "com.google.android.inputmethod.latin",
    "com.mi.vtalk",
    "com.android.midrive",
    "com.google.android.tts",
    "com.google.android.marvin.talkback",
    "com.xiaomi.pass",
    "com.miui.fmradio",
    "com.xiaomi.bluetooth",
    "com.google.android.feedback",
    "com.xiaomi.tag",
    "com.miui.miwallpaper",
    "com.facebook.services",
    "com.facebook.system",
    "com.facebook.appmanager"
  )

  def cutword(word:String): String ={
    val norword = WordNormalizer.normalize(word)
    if(word!=null && word.length >= 5){
      var tmp:String = word
      if (ToAnalysis.parse(word).getTerms().size() >= 1)  tmp=ToAnalysis.parse(word).getTerms().get(0).getName
      return tmp
    }
    else  return word
   }


  def filter_systemApp(word:String):Boolean={
    if(illegalAppset.contains(word)) return false
    else return true
  }


  def main(args: Array[String]): Unit = {
    val argv = Args(args)
    execute(argv, new SparkConf())
  }

  def execute(args:Args ,sparkConf: SparkConf): Unit = {
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._


    //create a map [packagename:(displayname,label)]
    val appinfo = spark.read.parquet(args("expansion"))
      .as[appkeyInfo]
      .map(f => {
        val pName = f.packageName
        val dName = f.displayName
        val label = Array(cutword(f.level1CategoryName), cutword(f.level2CategoryName)).mkString("#")
        (pName, (dName, label))
      }).distinct().rdd.filter(x=>filter_systemApp(x._1))

    val broaddict = spark.sparkContext.broadcast(appinfo.collectAsMap())


    /**get user's info  imei userGender and age**/
    val userinfo = spark.read.parquet(args("userinfo"))
      .map { x =>
        val imei = x.getAs[String]("id")
        val userGender = x.getAs[Byte]("userGender")
        val userAge = x.getAs[Byte]("userAge")
        (imei, userGender, userAge)
      }
      .toDF("imei", "userGender", "userAge")

    val res = spark.read.parquet(args("appusedaily"))
      .flatMap(x => {
        val id = x.getAs[String]("id")
        val imei_app_duration = x.getAs[Map[String, Map[String, Long]]]("appUsage")
          .map(e => {
            val app_packname = e._1
            val app_duration = e._2.getOrElse("usageDuration", 0.toLong)
            (id, app_packname, app_duration)
          })
        imei_app_duration
      }).rdd.map(x => ((x._1, x._2), x._3))
      .groupByKey()
      .map(x => (x._1, x._2.toArray.sum))
      .map(x => (x._1._1, (x._1._2, x._2)))
      .groupByKey().map(x => {
      val imei = x._1
      var top1appName = ""
      var top1appLabel = ""
      var top1_top4appLabel = ""
      var top5_top7appLabel = ""
      var top8_top11appLabel = ""
      var top12_top20appLabel = ""
      var top2_top3appName = ""
      var otherlabel = ""
      val others = new ArrayBuffer[String]()

      //put all app,usetime in array
      val appname_usetime = new ArrayBuffer[(String, Long)]()
      for (item <- x._2) appname_usetime.append((item._1, item._2))
      val app_usetime_sorted = appname_usetime.sortBy(_._2).reverse

      /** generate others contains 20 app's name and label **/
      for (i <- 0.to(math.min(19, app_usetime_sorted.length - 1))) {
        if (broaddict.value.contains(app_usetime_sorted(i)._1)) others.append(broaddict.value(app_usetime_sorted(i)._1)._1)
        val tmpapplabel = if (broaddict.value.contains(app_usetime_sorted(i)._1)) broaddict.value(app_usetime_sorted(i)._1)._2 else "None"
        others.append(tmpapplabel)
      }
      otherlabel = others.mkString("#")


      /** generate top1appName and  top1appLabel **/
      if (app_usetime_sorted.length > 0) {
        if (broaddict.value.contains(app_usetime_sorted(0)._1)) {
          top1appName = broaddict.value(app_usetime_sorted(0)._1)._1
          top1appLabel = broaddict.value(app_usetime_sorted(0)._1)._2
        }
        else {
          top1appName = "None"
          top1appLabel = "None"
        }
      }

      /** genereate top1-4 app's labels  **/
      for (i <- 0.to(math.min(3, app_usetime_sorted.length - 1))) {
        val tmpappname = app_usetime_sorted(i)._1
        val tmpapplabel = if (broaddict.value.contains(tmpappname)) broaddict.value(tmpappname)._2 else "None"
        top1_top4appLabel += tmpapplabel + "#"
      }

      /** generate top5_top7appLabel **/
      for (i <- 4.to(math.min(6, app_usetime_sorted.length - 1))) {
        val tmpappname = app_usetime_sorted(i)._1
        val tmpapplabel = if (broaddict.value.contains(tmpappname)) broaddict.value(tmpappname)._2 else "None"
        top5_top7appLabel += tmpapplabel + "#"
      }

      /** generate   top8_top11appLabel **/
      for (i <- 7.to(math.min(10, app_usetime_sorted.length - 1))) {
        val tmpappname = app_usetime_sorted(i)._1
        val tmpapplabel = if (broaddict.value.contains(tmpappname)) broaddict.value(tmpappname)._2 else "None"
        top8_top11appLabel += tmpapplabel + "#"
      }

      /** generate   top12_top20appLabel **/
      for (i <- 11.to(math.min(19, app_usetime_sorted.length - 1))) {
        val tmpappname = app_usetime_sorted(i)._1
        val tmpapplabel = if (broaddict.value.contains(tmpappname)) broaddict.value(tmpappname)._2 else "None"
        top12_top20appLabel += tmpapplabel + "#"
      }

      /** generate   top2_top3appName **/
      for (i <- 1.to(math.min(2, app_usetime_sorted.length - 1))) {
        val tmpappEname = app_usetime_sorted(i)._1
        val tmpappCname = if (broaddict.value.contains(tmpappEname)) broaddict.value(tmpappEname)._1 else "None"
        top2_top3appName += tmpappCname + "#"
      }

      /** generate  total appnum */
      val applength = app_usetime_sorted.length

      (imei, applength, top1appName, top1appLabel, top1_top4appLabel, top2_top3appName, top5_top7appLabel, top8_top11appLabel, top12_top20appLabel, otherlabel)
    })
      .toDF("imei", "appnum", "top1appName", "top1appLabel", "top1_top4appLabel", "top2_top3appName", "top5_top7appLabel", "top8_top11appLabel", "top12_top20appLabel", "otherlabels")
      .join(userinfo, "imei")
      .write.mode(SaveMode.Overwrite).parquet(args("output"))
  }
}
