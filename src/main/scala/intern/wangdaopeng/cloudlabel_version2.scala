package intern.wangdaopeng
import com.twitter.scalding.Args
import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.xiaomi.ad.matrix.qu.wordnormalizer.WordNormalizer
import com.xiaomi.intern.wangdaopeng.cloudlabel.{cutword, element}

import scala.collection.mutable.ArrayBuffer

object cloudlabel_version2 {

  case class appkeyInfo(packageName:String, displayName:String, level1CategoryName:String ,level2CategoryName:String )


  def cutword(word:String): String ={
    val norword = WordNormalizer.normalize(word)
    if(word.length >= 5){
      var tmp:String = word
      if (ToAnalysis.parse(word).getTerms().size() >= 1)  tmp=ToAnalysis.parse(word).getTerms().get(0).getName
      return tmp
    }
    else  return word
   }

  def main(args: Array[String]): Unit = {
    val argv = Args(args)
    execute(argv, new SparkConf())
  }

  def execute(args:Args ,sparkConf: SparkConf): Unit ={
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._


    //create a map [packagename:(displayname,label)]
    val appinfo = spark.read.parquet(args("expansion"))
      .as[appkeyInfo]
      .map(f=>{
          val pName = f.packageName
          val dName = f.displayName
          val label = Array(f.level1CategoryName, f.level2CategoryName).mkString("#")
          (pName,(dName,label))
      }).distinct().rdd
    val broaddict = spark.sparkContext.broadcast(appinfo.collectAsMap())


    //get user's info  imei userGender and age
    val userinfo  =spark.read.parquet(args("userinfo"))
      .map{x=>
        val imei = x.getAs[String]("id")
        val userGender = x.getAs[Byte]("userGender")
        val userAge = x.getAs[Byte]("userAge")
        (imei,userGender,userAge)
      }
      .toDF("imei","userGender","userAge")

    val res = spark.read.parquet(args("appusedaily"))
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
        .groupByKey().map(x=> {
            val imei =  x._1
            var top1appName = ""
            var top1appLabel= ""
            var top1_top4appLabel = ""
            var top5_top7appLabel = ""
            var top8_top11appLabel = ""
            var top12_top20appLabel = ""
            var top2_top3appName = ""
            val others =  new ArrayBuffer[String]()

       //put all app,usetime in array
      val appname_usetime = new ArrayBuffer[(String,Long)]()
      for(item<- x._2) appname_usetime.append((item._1,item._2))
      val app_usetime_sorted =  appname_usetime.sortBy(_._2).reverse

      /**others contains 20 app's name and label **/
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
         if(broaddict.value.contains(app_usetime_sorted(0)._1)) {
              top1appLabel =  broaddict.value(app_usetime_sorted(0)._1)._1
              top1appLabel =  broaddict.value(app_usetime_sorted(0)._1)._2
      }





















    }
    }





















  }






}
