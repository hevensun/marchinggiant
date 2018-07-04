package com.xiaomi.ad.keyword.marchinggiant.adinfo
/**
  */
import com.twitter.scalding.Args
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.util.Try

object GetAdIdInfo {
  case class help(cateId:String, score: Double)
  case class userResult(
                         imei1Md5: String, gCatSeq: Seq[help],  topicSeq: Seq[help], emiCatSeq: Seq[help], kwSeq:Seq[help]
                       )
  case class Category(id: Int, catName: String, score: Double)
  case class EmiCategory(catName: String, score: Double)
  case class adResult(appId:String, gCatSeq:Seq[help], emiCatSeq:Seq[help], topicSeq:Seq[help])
  case class LDATopic(topicId: Int, score: Double, topicDesc: String)
  case class AppInfo(
                      packageName: String,
                      appId: Long,
                      level1Category: String,
                      level2Category: String,
                      displayName: String,
                      keyWords: Seq[String],
                      levelClassifies: Seq[Category],
                      keywordsClassifies: Seq[Category],
                      keywordsTopicInfo: Seq[LDATopic],
                      introduction: String,
                      brief: String
                    )
    case class AssetInfo(
                            titles:Seq[String],
                            imgUrls:Seq[String],
                            level:Int,
                            titlesClassifies:Seq[Category],
                            assetId:Long
                        )
  case class adInfoTerm(adId: Long,level1Industry: String, level2Industry:String, appinfo: AppInfo, levelClassifies:Seq[Category], tagsClassifies:Seq[Category], tagsTopicInfo:Seq[LDATopic],emiClassifies: Seq[EmiCategory], adTopicInfo:Seq[LDATopic],assetInfo:AssetInfo)
  case class adWithAppTerm(adId: Long, appId: Long, googleCategory:Seq[Category], emiCategory: Seq[EmiCategory], lda:Seq[LDATopic])

  case class GoogleAppCate(appId: Long, gCatSeq:Seq[help])
  case class EmiAppCate(appId: Long, emiCatSeq:Seq[help])
  case class LdaAppCate(appId: Long, topicSeq:Seq[help])

  def main(args: Array[String]): Unit = {
    val argv = Args(args)
    val sparkConf = new SparkConf()
//    sparkConf.setMaster("local")
    execute(argv, sparkConf)
  }
    def fetchFirstCate(cateList: Seq[Category]): String = {
        val result = cateList.map{  t =>
            t.catName.split("/").head
        }.filter(_.nonEmpty)
        if (result.nonEmpty) result.head else "NULL"
    }

    def fetchSecondCate(cateList: Seq[Category]): String = {
        val result = cateList.map{  t =>
            val itemList = t.catName.split("/")
            if(itemList.size > 1){
                itemList(0) + ":" + itemList(1)
            }else{
                ""
            }
        }.filter(_.nonEmpty)
        if (result.nonEmpty) result.head else "NULL"
    }



  def execute(args: Args, sparkConf: SparkConf) = {
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    val appAndCate = spark.read.parquet(args("input_adinfo"))
      .filter($"appInfo".isNotNull && $"assetInfo".isNotNull)
      .select($"adId", $"level1Industry", $"level2Industry",$"appInfo", $"levelClassifies", $"tagsClassifies",$"tagsTopicInfo", $"emiClassifies", $"adTopicInfo",$"assetInfo")
      .as[adInfoTerm]
      .map{ m =>
          val adId = m.adId.toString
          val appName = m.appinfo.packageName
          val appId = Try(m.appinfo.appId).getOrElse(0)
          val advertiser_l1 = m.level1Industry
          val advertiser_l2 = m.level2Industry
          val app_l1 = Try(m.appinfo.level1Category).getOrElse("-")
          val app_l2 = Try(m.appinfo.level2Category).getOrElse("-")
          val app_levelC = if(m.appinfo.levelClassifies==null) Seq() else m.appinfo.levelClassifies

          val app_levelC_fc = fetchFirstCate(app_levelC)
          val app_levelC_sc = fetchSecondCate(app_levelC)

          val app_levelCStr = app_levelC.map{ m =>
              m.id.toString + ":" + m.catName + ":" + m.score.toString
          }.mkString("#")
          val adlevelC = if(m.levelClassifies==null) Seq() else m.levelClassifies
          val adlevelC_fc = fetchFirstCate(adlevelC)
          val adlevelC_sc = fetchSecondCate(adlevelC)

          val adlevelCStr = adlevelC.map{ m =>
              m.id.toString + ":" + m.catName + ":" + m.score.toString
          }.mkString("#")

          val adtagslevelC = if(m.tagsClassifies==null) Seq() else m.tagsClassifies
          val adtagslevelC_fc = fetchFirstCate(adtagslevelC)
          val adtagslevelC_sc = fetchSecondCate(adtagslevelC)


          val adtagsCStr = adtagslevelC.map{ m =>
              m.id.toString + ":" + m.catName + ":" + m.score.toString
          }.mkString("#")


          val app_keywordsC = if(m.appinfo.keywordsClassifies==null) Seq() else m.appinfo.keywordsClassifies
          val app_keywordsC_fc = fetchFirstCate(app_keywordsC)
          val app_keywordsC_sc = fetchSecondCate(app_keywordsC)

          val all_fc_sc = app_levelC_fc + "\t" + app_levelC_sc + "\t" +  adlevelC_fc  + "\t" + adlevelC_sc  + "\t" + adtagslevelC_fc  + "\t" + adtagslevelC_sc + "\t" + app_keywordsC_fc  + "\t" + app_keywordsC_fc

          val appkwCStr = app_keywordsC.map{ m =>
              m.id.toString + ":" + m.catName + ":" + m.score.toString
          }.mkString("#")

          // 判断是否为空 lda topic
          val tagsT = if(m.tagsTopicInfo==null) Seq() else m.tagsTopicInfo
          val adT = if(m.adTopicInfo==null) Seq() else m.adTopicInfo
          val keywordsT = if(m.appinfo.keywordsTopicInfo==null) Seq() else m.appinfo.keywordsTopicInfo

          val tagsTStr = tagsT.map{ m =>
              m.topicId + ":" + m.score.toString
          }.mkString("#")

          val adTStr = adT.map{ m =>
              m.topicId + ":" + m.score.toString
          }.mkString("#")

          val kwTStr = keywordsT.map{ m =>
              m.topicId + ":" + m.score.toString
          }.mkString("#")

          //  emi category
          val emiCategory = if(m.emiClassifies == null) Seq() else m.emiClassifies
          val emiCatStr = emiCategory.map{ m =>
              m.catName + ":" + m.score.toString
          }.mkString("#")

          val assetTitlesC = if(m.assetInfo.titlesClassifies==null) Seq() else m.assetInfo.titlesClassifies
          val assetIdStr = m.assetInfo.assetId.toString
          //val assetTitlesC = m.assetInfo.titlesClassifies
          val assetTitlesC_fc = fetchFirstCate(assetTitlesC)
          val assetTitlesC_sc = fetchSecondCate(assetTitlesC)

          s"$adId\t$appId\t$appName\t$assetIdStr\t$assetTitlesC_fc\t$assetTitlesC_sc\t$all_fc_sc\t$advertiser_l1\t$advertiser_l2\t$app_l1\t$app_l2\t$adtagsCStr\t$adlevelCStr\t$app_levelCStr\t$appkwCStr\t$tagsTStr\t$adTStr\t$kwTStr\t$emiCatStr"
      }
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .text(args("output"))

    spark.stop()
  }
}

