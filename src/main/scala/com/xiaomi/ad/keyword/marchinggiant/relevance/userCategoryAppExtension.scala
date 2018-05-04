package com.xiaomi.ad.keyword.marchinggiant.relevance

import com.twitter.scalding.Args
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.xiaomi.ad.qu.get_query_term_weight.usage.{getTermImp, getTermWeight}
import org.apache.spark

import collection.JavaConverters._
import scala.util.Try

/**
  * create by liguoyu on 2018-04-23
  * 该类在原有基础上添加 app category (google ,emi)和 lda topic 以及 app keywords
  *
  */

object userCategoryAppExtension {
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
                     imei1Md5: String, gCatSeq: Seq[Term],  topicSeq: Seq[Term], emiCatSeq: Seq[Term], kwSeq:Seq[Term]
                   )

  case class ResultAppExtension(
                     imei1Md5: String,
                     gCatSeq: Seq[Term],
                     topicSeq: Seq[Term],
                     emiCatSeq: Seq[Term],
                     kwSeq:Seq[Term],
                     appGoogleCatSeq: Seq[Term],
                     appTopicSeq: Seq[Term],
                     appEmiCatSeq: Seq[Term],
                     appKeyWordsSeq:Seq[String]
                   )
  def main(args: Array[String]): Unit = {
    val argv = Args(args)
    val conf = new SparkConf()
    execute(argv, conf)
  }

  def getGoogleCateSeq(data: Seq[BehaviorTag], cateExtendB: Map[String, Seq[String]]): Seq[String] ={
    val google = data.flatMap { m =>
      val ss = m.extension.getOrElse("2", "0:0").split(" ")
        .flatMap { mm =>
          val term = mm.split(":")
          val key = term(0)
          val value = term(1).toDouble
          val extend = key +: cateExtendB.getOrElse(key, Seq())
          val sss = extend.map{ mmm =>
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
          cate+":"+(score / size).toString
        }.toSeq
      ss
    }
    google
  }

  def getEmiOrTopic(splitC: String, data: Seq[BehaviorTag]): Seq[String] ={
    val seqCate = data.flatMap { m =>
      val ss = m.extension.getOrElse(splitC, "0:0").split(" ")
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
          cate+":"+(score / size).toString
        }.toSeq
      ss
    }
    seqCate
  }

  def getAppCateOrTopic(data: Seq[BehaviorTag], packageMap: Map[String, Seq[(String, Double)]], appIdMap: Map[String, Seq[(String, Double)]]): Seq[String] ={
    data.flatMap{rows=>
      val sourceId = rows.sourceId
      val actionId = rows.actionId
      val re1 = if(sourceId == 3){
        val packageName = rows.entityKey.split('|').last
        if(packageMap.contains(packageName))
        {
          packageMap.get(packageName).get
        }
        else Seq()
      }
      else if(sourceId == 5 && (actionId == 1|| actionId == 4)){
        val appid = rows.text
        if(appIdMap.contains(appid))
        {
          appIdMap.get(appid).get
        }
        else Seq()
      }
      else Seq()
      re1
    }.groupBy(f=>f._1)
      .map{row=>
        val catId = row._1
        val size = row._2.size
        val score = row._2.map{r=>r._2}.sum
        catId.toString+":"+(score/size).toString
      }.toSeq
  }

  def getAppCateExtension(data: Seq[BehaviorTag], packageMap: Map[String, Seq[(String, Double)]], appIdMap: Map[String, Seq[(String, Double)]], cateExtendB: Map[String, Seq[String]]): Seq[String] ={
    data.flatMap{rows=>
      val sourceId = rows.sourceId
      val actionId = rows.actionId
      val re1 = if(sourceId == 3){
        val packageName = rows.entityKey.split('|').last
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
        val appid = rows.text
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
    }.groupBy(f=>f._1)
      .map{row=>
        val catId = row._1
        val size = row._2.size
        val score = row._2.map{r=>r._2}.sum
        catId.toString+":"+(score/size).toString
      }.toSeq
  }

  // get Term for cate emi lda (0-5 : gcate, emiCate, ldaTopic, appGcate, appEmiCate, appLdaTopic)
  def getTerm(data: Seq[Seq[Seq[String]]], typeC: Int): Seq[Term] ={
    data.flatMap{r=>
      val cate = r(typeC).map{r=>
        val term = r.split(":")
        val cateId = term(0)
        val score = term(1).toDouble
        (cateId, score)
      }
      cate
    }.groupBy(_._1).map{case(key, value)=>
      val cateId = key
      val size = value.size
      val score = value.map{a=>a._2}.sum
      Term(cateId, score/size)
    }.toSeq
  }

  def getKeyWordWeigth(query: String, qtw: getTermWeight, gtm: getTermImp): Seq[Term] ={
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
      .filter(f => (f.sourceId == 3 || (f.sourceId == 5&&(f.actionId==1||f.actionId==4))) && !appSetB.value.contains(f.entityKey))

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
    val tt = matrix.filter(f => !userFilterB.value.contains(f.imei1Md5))
      .mapPartitions{rows=>
        rows.map{r=>
          val randomNum = 1000+rndBc.value.nextInt(8999)
          val imei = randomNum.toString+r.imei1Md5
          imei->r
        }
      }
      .groupByKey(_._1)
      .mapGroups { case (imei, bts) =>
        val value_bts = bts.map{rr=>rr._2}.toSeq
        val imeiMd5 = value_bts.head.imei1Md5
        val google = getGoogleCateSeq(value_bts, cateExtendB.value)

        val topic = getEmiOrTopic("3", value_bts)

        val emi = getEmiOrTopic("6", value_bts)

        val kwq = value_bts.filter(f => (!f.text.isEmpty && f.text != None))
          .flatMap { mm =>
            val query = mm.text.trim.replace(" ", "")
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
          }.map(r=>r.cate+":"+r.score.toString)

        //添加 app extension category; emicategory; lda topic; keywords
        //添加 app extension category;
        val appGoogleCat = getAppCateExtension(value_bts,appCategoryMapPackageBC.value, appCategoryMapAppIdBC.value, cateExtendB.value)

        val appEmiCat = getAppCateOrTopic(value_bts,appEmiCategoryMapPackageBC.value, appEmiCategoryMapPackageBC.value)
        //添加 app extension lda topic;
        val appLdaTopic = getAppCateOrTopic(value_bts,appLdaTopicMapPackageBC.value, appLdaTopicMapAppIdBC.value)
        //添加 app extension key word;
        val appKeywords = value_bts.flatMap{rows=>
          val sourceId = rows.sourceId
          val actionId = rows.actionId
          val re1 = if(sourceId == 3){
            val packageName = Try(rows.entityKey.split('|').last).getOrElse("")
            val state = appKeywordsMapPackageBC.value.contains(packageName)
            if(state){
              val keywords = appKeywordsMapPackageBC.value.get(packageName).get
              if(keywords.isEmpty) Seq()
              else keywords
            }
            else Seq()
          }
          else if(sourceId == 5&& (actionId==1||actionId==4)){
            val appid = Try(rows.text).getOrElse("")
            val state = appKeywordsMapAppIdBC.value.contains(appid)
            if(state){
              val keywords = appKeywordsMapAppIdBC.value.get(appid).get
              if(keywords.isEmpty) Seq()
              else keywords
            }
            else Seq()
          }
          else Seq()
          re1
        }.toSet.toSeq
        val appKeyWords = if(appKeywords==null||appKeywords.isEmpty) Seq()
        else appKeywords.map{r=>
          val word = r
          val score = qtw.getWeight(word)
          word+":"+score.toString
        }
        imeiMd5->Seq(google, topic, emi, kwq, appGoogleCat, appLdaTopic, appEmiCat, appKeyWords)
      }.groupByKey(_._1)
      .mapGroups{case(key, value_bts)=>
        val imei = key
        val value_seq = value_bts.map{r=>r._2}.toSeq
        val googleCate = getTerm(value_seq, 0)
        val ldaTopic = getTerm(value_seq,1)
        val emiCategory = getTerm(value_seq,2)
        val keyWords = getTerm(value_seq, 3)
        val appGoogleCate = getTerm(value_seq, 4)
        val appLdaTopic = getTerm(value_seq, 5)
        val appEmiCate = getTerm(value_seq, 6)
        val appKeywords = value_seq.flatMap{rr=>
          rr(7)
        }
        ResultAppExtension(imei, googleCate, ldaTopic, emiCategory, keyWords, appGoogleCate, appLdaTopic, appEmiCate, appKeywords)
      }
      tt.filter(r => r.gCatSeq.nonEmpty || r.emiCatSeq.nonEmpty || r.topicSeq.nonEmpty||r.appEmiCatSeq.nonEmpty||r.appGoogleCatSeq.nonEmpty||r.appTopicSeq.nonEmpty||r.appKeyWordsSeq.nonEmpty)
      .repartition(500)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(args("output"))
    println("Task Finished")
    spark.stop()
  }
}
