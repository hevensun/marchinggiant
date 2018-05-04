package com.xiaomi.ad.keyword.marchinggiant.relevance

import com.twitter.scalding.Args
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.xiaomi.ad.qu.get_query_term_weight.usage.{getTermImp, getTermWeight}

import collection.JavaConverters._
/**
  * create by liguoyu on 2018-04-28
  * 该类在原有基础上添加 app category (google ,emi)和 lda topic 以及 app keywords
  *
  */

object userCategoryAppExtension_T {

  case class Term(cate: String, score: Double)

  case class Category(catId: Int, catName: String, score: Double)

  case class EmiCategory(name: String, score: Double)

  case class LdaTopic(topicId: Int, topicName: String, score: Double)

  /**
    * @param appId
    * @param packageName
    * @param keywords
    * @param category
    * @param emiCategory
    * @param lda
    */

  case class AppExtensionNew(
                              appId: Long,
                              packageName: String,
                              keywords: Seq[String],
                              category: Seq[Category],
                              emiCategory: Seq[EmiCategory],
                              lda: Seq[LdaTopic]
                            )

  case class newBehaviorTag(
                             imei1AndTime: String,
                             timeStamp: Long,
                             sourceId: Int,
                             actionId: Int,
                             duration: Long,
                             text: String,
                             entityKey: String,
                             extension: scala.collection.Map[String, String]
                           )

  case class BehaviorTag(
                          imei1Md5: String,
                          timeStamp: Long,
                          sourceId: Int,
                          actionId: Int,
                          duration: Long,
                          text: String,
                          entityKey: String,
                          extension: scala.collection.Map[String, String],
                          date: Integer
                        )

  case class Result(
                     imei1Md5: String, gCatSeq: Seq[Term], topicSeq: Seq[Term], emiCatSeq: Seq[Term], kwSeq: Seq[Term]
                   )

  case class ResultAppExtension(
                                 imei1Md5: String,
                                 gCatSeq: Seq[Term],
                                 topicSeq: Seq[Term],
                                 emiCatSeq: Seq[Term],
                                 kwSeq: Seq[Term],
                                 appGoogleCatSeq: Seq[Term],
                                 appTopicSeq: Seq[Term],
                                 appEmiCatSeq: Seq[Term],
                                 appKeyWordsSeq: Seq[String]
                               )

  def main(args: Array[String]): Unit = {
    val argv = Args(args)
    val conf = new SparkConf()
    execute(argv, conf)
  }

  // get google category and extend those category
  def getGoogleCateSeq(data: BehaviorTag, cateExtendB: Map[String, Seq[String]]): Seq[String] = {
    val ss = data.extension.getOrElse("2", "0:0").split(" ")
      .flatMap { mm =>
        val term = mm.split(":")
        val key = term(0)
        val value = term(1).toDouble
        val extend = key +: cateExtendB.getOrElse(key, Seq())
        val sss = extend.map { mmm =>
          (mmm, value)
        }
        sss
      }.filter(f => f._1 != "0")
      .groupBy(_._1)
      .map { m =>
        val cate = m._1
        val size = m._2.size
        val score = m._2.map { mm =>
          mm._2
        }.sum
        cate + ":" + (score / size).toString
      }.toSeq
    ss
  }

  // get emi category or topic
  def getEmiOrTopic(splitC: String, data: BehaviorTag): Seq[String] = {
    val ss = data.extension.getOrElse(splitC, "0:0").split(" ")
      .map { mm =>
        val term = mm.split(":")
        val key = term(0)
        val value = term(1).toDouble
        key -> value

      }.filter(f => f._1 != "0")
      .groupBy(_._1)
      .map { m =>
        val cate = m._1

        val size = m._2.size
        val score = m._2.map { mm =>
          mm._2
        }.sum
        cate + ":" + (score / size).toString
      }.toSeq
    ss
  }

  // get app eim category or topic
  def getAppCateOrTopic(data: BehaviorTag, packageMap: Map[String, Seq[(String, Double)]], appIdMap: Map[String, Seq[(String, Double)]]): Seq[String] = {
    val sourceId = data.sourceId
    val actionId = data.actionId
    val re1 = if (sourceId == 3) {
      val packageName = data.entityKey.split('|').last
      if (packageMap.contains(packageName)) packageMap.get(packageName).get else Seq()
    }
    else if (sourceId == 5 && (actionId == 1 || actionId == 4)) {
      val appid = data.text
      if (appIdMap.contains(appid)) appIdMap.get(appid).get else Seq()
    }
    else Seq()
    re1.map{r=>
      r._1+":"+r._2.toString
    }
  }

  // get key words
  def getAppKeyWords(data: BehaviorTag, packageMap: Map[String, Seq[(String)]], appIdMap: Map[String, Seq[(String)]]): Seq[String] = {
    val sourceId = data.sourceId
    val actionId = data.actionId
    val re1 = if (sourceId == 3) {
      val packageName = data.entityKey.split('|').last
      if (packageMap.contains(packageName)) packageMap.get(packageName).get else Seq()
    }
    else if (sourceId == 5 && (actionId == 1 || actionId == 4)) {
      val appid = data.text
      if (appIdMap.contains(appid)) appIdMap.get(appid).get else Seq()
    }
    else Seq()
    re1
  }

  // get app category's extension categories
  def getAppCateExtension(data: BehaviorTag, packageMap: Map[String, Seq[(String, Double)]], appIdMap: Map[String, Seq[(String, Double)]], cateExtendB: Map[String, Seq[String]]): Seq[String] ={
    val sourceId = data.sourceId
    val actionId = data.actionId
    val re1 = if(sourceId == 3){
      val packageName = data.entityKey.split('|').last
      if(packageMap.contains(packageName))
      {
        packageMap.get(packageName).get
          .flatMap{a=>
            val cate = a._1
            val score = a._2
            val extend = cate +: cateExtendB.getOrElse(cate, Seq())
            val sss = extend.map{ mmm =>
              (mmm, score)
            }
            sss
          }.filter(f=>f._1 != "0")
      }
      else Seq()
    }
    else if(sourceId == 5 && (actionId == 1|| actionId == 4)){
      val appid = data.text
      if(appIdMap.contains(appid))
      {
        appIdMap.get(appid).get
          .flatMap{a=>
            val cate = a._1
            val score = a._2
            val extend = cate +: cateExtendB.getOrElse(cate, Seq())
            val sss = extend.map{ mmm =>
              (mmm, score)
            }
            sss
          }.filter(f=>f._1 != "0")
      }
      else Seq()
    }
    else Seq()
    re1
    .groupBy(f=>f._1)
      .map{row=>
        val catId = row._1
        val size = row._2.size
        val score = row._2.map{r=>r._2}.sum
        catId.toString+":"+(score/size).toString
      }.toSeq
  }

  // get Term for cate emi lda (0-5 : gcate, emiCate, ldaTopic, appGcate, appEmiCate, appLdaTopic)
  def getTerm(data: Seq[String]): Seq[Term] ={
    if(data == null||data.isEmpty) Seq()
    else data.map{r=>
      val term = r.split(":")
      val cateId = term(0)
      val score = term(1).toDouble
      (cateId, score)
    }.groupBy(_._1).map{case(key, value)=>
      val cateId = key
      val size = value.size
      val score = value.map{a=>a._2}.sum
      Term(cateId, score/size)
    }.toSeq
  }

  //merge and caculator arg value for same category id or topic
  def mergeCate(data: Seq[String]): Seq[String] ={
    if(data == null||data.isEmpty) Seq()
    else data.map{r=>
      val term = r.split(":")
      val cateId = term(0)
      val score = term(1).toDouble
      (cateId, score)
    }.groupBy(_._1).map{case(key, value)=>
      val cateId = key
      val size = value.size
      val score = value.map{a=>a._2}.sum
      cateId+":"+(score/size).toString
    }.toSeq
  }

  // get keywords' word weight
  def getKeyWordWeigth(query: String, qtw: getTermWeight, gtm: getTermImp): Seq[Term] ={
    if(query==null||query.isEmpty) Seq()
    else {
      val term1 = qtw.getWeight(query).asScala.map { t =>
        val term = t.split("\t")
        if (term.size == 2) {
          val key = term(0).trim
          val value = term(1).trim.toDouble
          Term(key, value)
        } else {
          Term("###", 0.000)
        }
      }
      val term2 = gtm.getTermImp(query).asScala.map { t =>
        val term = t.split("\t")
        if (term.size == 2) {
          val key = term(0).trim
          val value = term(1).trim.toDouble
          Term(key, value)
        } else {
          Term("###", 0.000)
        }
      }
      val nterm1 = term1.filter(f => f.cate != "###").toList
      val nterm2 = term2.filter(f => f.cate != "###").toList
      val term = nterm1 ++ nterm2
      term
    }
  }

  def execute(args: Args, sparkConf: SparkConf) = {
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    val qtw = new getTermWeight
    val gtm = new getTermImp

    val appSet = spark.read.text(args("input_appFilter"))
      .map { m =>
        m.getAs[String]("value")
      }.collect()
      .toSet

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

    val appSetB = spark.sparkContext.broadcast(appSet)

    val matrix = spark.read.parquet(args("input_matrix"))
      .as[BehaviorTag]
      .filter(f => (f.sourceId == 3 || f.sourceId == 5) && !appSetB.value.contains(f.entityKey))

    matrix.persist()

    val threshold = args("input_threshold").toInt

    val userFilter = matrix.map { m =>
      m.imei1Md5 -> 1
    }.rdd
      .reduceByKey(_ + _)
      .sortBy(s => s._2, false)
      .filter(f => f._2 > threshold)
      .map{ m =>
        m._1
      }.collect().toSet

    val appExtension = spark.read.parquet(args("appNews")).as[AppExtensionNew]

    // emi category map
    val appEmiCategoryMapAppId = appExtension.map{row=>
      val appid = row.appId.toString
      val category = row.emiCategory.map{a=>
        val catId = a.name
        val score = a.score
        (catId, score)
      }
      appid -> category
    }.collect().toMap
    val appEmiCategoryMapAppIdBC = spark.sparkContext.broadcast(appEmiCategoryMapAppId)

    val appEmiCategoryMapPackage = appExtension.map{row=>
      val packageName = row.packageName
      val category = row.emiCategory.map{a=>
        val catId = a.name
        val score = a.score
        (catId, score)
      }
      packageName -> category
    }.collect().toMap
    val appEmiCategoryMapPackageBC = spark.sparkContext.broadcast(appEmiCategoryMapPackage)

    // lda topic map
    val appLdaTopicMapPackage = appExtension.map{row=>
      val packageName = row.packageName
      val category = row.lda.map{a=>
        val topicId = a.topicId.toString
        val score = a.score
        (topicId, score)
      }
      packageName -> category
    }.collect().toMap
    val appLdaTopicMapPackageBC = spark.sparkContext.broadcast(appLdaTopicMapPackage)

    val appLdaTopicMapAppId = appExtension.map{row=>
      val appId = row.appId.toString
      val category = row.lda.map{a=>
        val topicId = a.topicId.toString
        val score = a.score
        (topicId, score)
      }
      appId -> category
    }.collect().toMap
    val appLdaTopicMapAppIdBC = spark.sparkContext.broadcast(appLdaTopicMapAppId)

    //category map
    val appCategoryMapAppId = appExtension.map{row=>
      val appid = row.appId.toString
      val category = row.category.map{a=>
        val catId = a.catId.toString
        val catName = a.catName
        val score = a.score
        (catId, score)
      }
      appid -> category
    }.collect().toMap
    val appCategoryMapAppIdBC = spark.sparkContext.broadcast(appCategoryMapAppId)

    val appCategoryMapPackage = appExtension.map{row=>
      val packageName = row.packageName
      val category = row.category.map{a=>
        val catId = a.catId.toString
        val catName = a.catName
        val score = a.score
        (catId, score)
      }
      packageName -> category
    }.collect().toMap
    val appCategoryMapPackageBC = spark.sparkContext.broadcast(appCategoryMapPackage)

    //关键字 map
    val appKeywordsMapAppId = appExtension.map{row=>
      val appid = row.appId.toString
      val category = if(row.keywords==null) Seq() else row.keywords
      appid -> category
    }.collect().toMap
    val appKeywordsMapAppIdBC = spark.sparkContext.broadcast(appKeywordsMapAppId)

    val appKeywordsMapPackage = appExtension.map{row=>
      val packageName = row.packageName
      val category = if(row.keywords==null) Seq() else row.keywords
      packageName -> category
    }.collect().toMap
    val appKeywordsMapPackageBC = spark.sparkContext.broadcast(appKeywordsMapPackage)

    val userFilterB = spark.sparkContext.broadcast(userFilter)
    val rnd = new scala.util.Random
    val rndBc = spark.sparkContext.broadcast(rnd)

    val hashStart = spark.sparkContext.broadcast(args("hashS").toInt)
    val hashRange = spark.sparkContext.broadcast(args("hashKey").toInt)

    val tt = matrix.filter(f => !userFilterB.value.contains(f.imei1Md5))
      .mapPartitions{rows=>
        rows.map{r=>
          val randomNum = hashStart.value + rndBc.value.nextInt(hashRange.value)
          val imei = randomNum.toString + r.imei1Md5
          val gCate = getGoogleCateSeq(r, cateExtendB.value)
          val emiCate = getEmiOrTopic("6", r)
          val ldaTopic = getEmiOrTopic("3", r)
          val keywords = getKeyWordWeigth(r.text, qtw, gtm)
          // 添加 app google category ;emi category; lda topic; key word
          val appGcate = getAppCateExtension(r,appCategoryMapPackageBC.value, appCategoryMapAppIdBC.value, cateExtendB.value)
          val appEmiCate = getAppCateOrTopic(r, appEmiCategoryMapPackageBC.value, appEmiCategoryMapAppIdBC.value)
          val appLdaTopic = getAppCateOrTopic(r, appLdaTopicMapPackageBC.value, appLdaTopicMapAppIdBC.value)
          val appkeywords = getAppKeyWords(r, appKeywordsMapPackageBC.value, appKeywordsMapAppIdBC.value)
          imei->(r.imei1Md5, gCate, emiCate, ldaTopic, keywords, appGcate, appEmiCate, appLdaTopic, appkeywords)
        }
      }
      .rdd
      .reduceByKey{(x, y)=>
        val imei = x._1
        val gCate = mergeCate(x._2 ++ y._2)
        val emiCate = mergeCate(x._3 ++ y._3)
        val lda = mergeCate(x._4 ++ y._4)
        val keywords = x._5 ++ y._5
        val appGCate = mergeCate(x._6 ++ y._6)
        val appEmiCate = mergeCate(x._7 ++ y._7)
        val appLda = mergeCate(x._8 ++ y._8)
        val appKeywords = x._9 ++ y._9
        (imei, gCate, emiCate, lda, keywords, appGCate, appEmiCate, appLda, appKeywords)
      }
      .mapPartitions{rows=>
        rows.map{r=>
          val line = r._2
          val imei = line._1
          imei-> line
        }
      }
      .reduceByKey{(x, y)=>
        val imei = x._1
        val gCate = mergeCate(x._2 ++ y._2)
        val emiCate = mergeCate(x._3 ++ y._3)
        val lda = mergeCate(x._4 ++ y._4)
        val keywords = x._5 ++ y._5
        val appGCate = mergeCate(x._6 ++ y._6)
        val appEmiCate = mergeCate(x._7 ++ y._7)
        val appLda = mergeCate(x._8 ++ y._8)
        val appKeywords = x._9 ++ y._9
        (imei, gCate, emiCate, lda, keywords, appGCate, appEmiCate, appLda, appKeywords)
      }.mapPartitions{rows=>
        rows.map{r=>
          val imei = r._1
          val x = r._2
          val gCate = getTerm(x._2)
          val emiCate = getTerm(x._3)
          val lda = getTerm(x._4)
          val keywords = x._5
          val appGCate = getTerm(x._6)
          val appEmiCate = getTerm(x._7)
          val appLda = getTerm(x._8)
          val appKeywords = x._9
          ResultAppExtension(imei, gCate, lda, emiCate, keywords, appGCate, appLda, appEmiCate, appKeywords)
        }
      }.toDS().as[ResultAppExtension]

    tt.filter(r => r.gCatSeq.nonEmpty || r.emiCatSeq.nonEmpty || r.topicSeq.nonEmpty||r.appEmiCatSeq.nonEmpty||r.appGoogleCatSeq.nonEmpty||r.appTopicSeq.nonEmpty||r.appKeyWordsSeq.nonEmpty)
      .repartition(500)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(args("output"))
    println("Task Finished")
    spark.stop()
  }
}
