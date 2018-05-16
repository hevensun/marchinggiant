package com.xiaomi.ad.keyword.marchinggiant.relevance.infofeed

/**
  * Create by liguoyu on 2018-04-19
  * Fun:
  *   输入： ad info ： 广告数据; extension key -> value  : category id 拓展信息数据 ;
  *   注： 如果觉得 ad 数量太大，可添加过滤 code 已注释
  *   1. 从 ad info feed 数据中提取category ,emiCategory ,lda;
  *   2. 对category 添加 extension category ;
  *   3. 根据category score 计算 extension category score
  *   4. 输出结果如 ：adInfoTerm 定义所示。
  */

import com.twitter.scalding.Args
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

object adInfoCategory {
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
  case class adInfoTerm(adId: Long, appinfo: AppInfo, levelClassifies:Seq[Category], emiClassifies: Seq[EmiCategory], adTopicInfo:Seq[LDATopic])
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


  def execute(args: Args, sparkConf: SparkConf) = {
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    val cateExtend = spark.read.text(args("input_cateExtend"))
      .map{ m =>
        val ss = m.getAs[String]("value")
        if ( ss.split("\t").length > 1) {
          val key = ss.split("\t")(0)
          val value = ss.split("\t").drop(1).toSeq
          key -> value
        } else {
          "###" -> List("***")
        }
      }.filter(f => f._1 != "###")
      .collect()
      .toMap

    val cateExtendB = spark.sparkContext.broadcast(cateExtend)

    val cateThread = spark.sparkContext.broadcast(args("CateThread").toDouble)

    val appAndCate = spark.read.parquet(args("input_adinfo"))
      .select($"adId", $"appInfo", $"levelClassifies", $"emiClassifies", $"adTopicInfo")
    val appAndCateGroup = appAndCate.as[adInfoTerm].filter($"appInfo".isNotNull)
      .map { m =>
        val adId = m.adId
        val appId = m.appinfo.appId
        // 判断是否为空
        val keywordsC = if(m.appinfo.keywordsClassifies==null) Seq() else m.appinfo.keywordsClassifies
        val levelC = if(m.appinfo.levelClassifies==null) Seq() else m.appinfo.levelClassifies
        val adlevelC = if(m.levelClassifies==null) Seq() else m.levelClassifies
        val adWithAppCategoryG = keywordsC ++ levelC ++ adlevelC

        // 判断是否为空
        val keywordsT = if(m.appinfo.keywordsTopicInfo==null) Seq() else m.appinfo.keywordsTopicInfo
        val adT = if(m.adTopicInfo==null) Seq() else m.adTopicInfo
        val adWithAppLdaTopic = adT ++ keywordsT
        val emiCategory = if(m.emiClassifies == null) Seq() else m.emiClassifies
        (appId, adWithAppTerm(adId, appId, adWithAppCategoryG, emiCategory, adWithAppLdaTopic))
      }.groupByKey(_._1)

    val googleAppCate = appAndCateGroup.mapGroups { case (key, value) =>
      val appId = key
      val data = value
      // google category extend
      val googleCate = data.flatMap { terms =>
        val cate = terms._2.googleCategory.map { r =>
          val cateId = r.id
          val score = r.score
          help(cateId.toString, score)
        }.flatMap { g =>
          val key = g.cateId
          val value = g.score
          val extend = key +: cateExtendB.value.getOrElse(key, Seq())
          val ss = extend.map { mm =>
            help(mm, value)
          }
          ss
        }
        cate
      }.toSeq.filter(f => f.score > cateThread.value)
        .groupBy(b => b.cateId)
        .map { m =>
          val cate = m._1
          val size = m._2.size
          val score = m._2.map(x => x.score).sum
          help(cate, score / size)
        }.toSeq
      GoogleAppCate(appId, googleCate)
    }
    //emiCategory
    val emiAppCate = appAndCateGroup.mapGroups { case (key, value) =>
      val appId = key
      val data = value
      val emiCategory = data
        .flatMap { terms =>
          val emiCategory = terms._2.emiCategory
          emiCategory
        }.toSeq.filter(f=>f.score > cateThread.value)
        .groupBy(_.catName)
        .map { r =>
          val emiCateName = r._1
          val emiCateSize = r._2.size
          val score = r._2.map(x => x.score).sum / emiCateSize
          help(emiCateName, score)
        }.toSeq
      EmiAppCate(appId, emiCategory)
    }

    // lda topic
    val ldaApptopic = appAndCateGroup.mapGroups { case (key, value) =>
      val appId = key
      val data = value
      val ldaTopic = data
        .flatMap(terms => terms._2.lda).toSeq
        .groupBy(f => f.topicId).map { r =>
        val topicId = r._1
        val topicSizeNum = r._2.size
        val topicName = r._2(0).topicDesc
        val score = r._2.map(x => x.score).sum / topicSizeNum
        help(topicId.toString, score)
      }.toSeq
      LdaAppCate(appId, ldaTopic)
    }

    val googleAndEmiAppCate = googleAppCate.join(emiAppCate, Seq("appId"))
    val allCateAndTopic = googleAndEmiAppCate.join(ldaApptopic, Seq("appId"))
//    allCateAndTopic.show(false)
    allCateAndTopic.as[adResult]
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(args("output"))

    spark.stop()
  }

}

