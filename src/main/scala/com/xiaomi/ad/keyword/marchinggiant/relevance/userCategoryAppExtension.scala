package com.xiaomi.ad.keyword.marchinggiant.relevance

import com.twitter.scalding.Args
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.xiaomi.ad.qu.get_query_term_weight.usage.{getTermImp, getTermWeight}

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
    * root
 |-- appId: long (nullable = true)
 |-- packageName: string (nullable = true)
 |-- keywords: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- category: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- catId: integer (nullable = true)
 |    |    |-- catName: string (nullable = true)
 |    |    |-- score: double (nullable = true)
 |-- emiCategory: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- name: string (nullable = true)
 |    |    |-- score: double (nullable = true)
 |-- lda: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- topicId: integer (nullable = true)
 |    |    |-- topicName: string (nullable = true)
 |    |    |-- score: double (nullable = true)

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
    // for  test on local machine
//    conf.setMaster("local")
    execute(argv, conf)
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
      .filter(f => (f.sourceId == 2 || f.sourceId == 3 || f.sourceId == 5 || f.sourceId == 7) && !appSetB.value.contains(f.entityKey))

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
      }.collect().toSeq

    val appExtension = spark.read.parquet(args("appNews")).as[AppExtensionNew]

    // emi category map
    val appEmiCategoryMapAppId = appExtension.map{row=>
      val appid = row.appId.toString
      val category = row.emiCategory.toSeq.map{a=>
        val catId = a.name
        val score = a.score
        (catId, score)
      }
      appid -> category
    }.collect().toMap
    val appEmiCategoryMapAppIdBC = spark.sparkContext.broadcast(appEmiCategoryMapAppId)

    val appEmiCategoryMapPackage = appExtension.map{row=>
      val packageName = row.packageName
      val category = row.emiCategory.toSeq.map{a=>
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
      val category = row.lda.toSeq.map{a=>
        val topicId = a.topicId
        val score = a.score
        (topicId, score)
      }
      packageName -> category
    }.collect().toMap
    val appLdaTopicMapPackageBC = spark.sparkContext.broadcast(appEmiCategoryMapPackage)

    val appLdaTopicMapAppId = appExtension.map{row=>
      val packageName = row.packageName
      val category = row.lda.toSeq.map{a=>
        val topicId = a.topicId
        val score = a.score
        (topicId, score)
      }
      packageName -> category
    }.collect().toMap
    val appLdaTopicMapAppIdBC = spark.sparkContext.broadcast(appLdaTopicMapAppId)

    //category map
    val appCategoryMapAppId = appExtension.map{row=>
      val appid = row.appId.toString
      val category = row.category.toSeq.map{a=>
        val catId = a.catId.toString
        val catName = a.catName
        val score = a.score
        (catId, catName, score)
      }
      appid -> category
    }.collect().toMap
    val appCategoryMapAppIdBC = spark.sparkContext.broadcast(appCategoryMapAppId)

    val appCategoryMapPackage = appExtension.map{row=>
      val packageName = row.packageName
      val category = row.category.toSeq.map{a=>
        val catId = a.catId
        val catName = a.catName
        val score = a.score
        (catId, catName, score)
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

    val tt = matrix.filter(f => !userFilterB.value.contains(f.imei1Md5))
      .groupByKey(_.imei1Md5)
      .mapGroups { case (imei, bts) =>
        val value_bts = bts.toArray
        val google = value_bts.flatMap { m =>
          val ss = m.extension.getOrElse("2", "0:0").split(" ")
            .flatMap { mm =>
              val key = mm.split(":")(0)
              val value = mm.split(":")(1).toDouble
              val extend = key +: cateExtendB.value.getOrElse(key, Seq())
              val sss = extend.map{ mmm =>
                mmm -> value
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
              Term(cate, score / size)
            }.toSeq
          ss
        }.toSeq

        val topic = value_bts.flatMap { m =>
          val ss = m.extension.getOrElse("3", "0:0").split(" ")
            .map { mm =>
              val key = mm.split(":")(0)
              val value = mm.split(":")(1).toDouble
              key -> value
            }.filter(f => f._1 != "0")
            .groupBy(_._1)
            .map { m =>
              val cate = m._1
              val size = m._2.size
              val score = m._2.map { mm =>
                mm._2
              }.sum
              Term(cate, score / size)
            }.toSeq
          ss
        }.toSeq

        val emi = value_bts.flatMap { m =>
          val ss = m.extension.getOrElse("6", "0:0").split(" ")
            .map { mm =>
              val key = mm.split(":")(0)
              val value = mm.split(":")(1).toDouble
              key -> value

            }.filter(f => f._1 != "0")
            .groupBy(_._1)
            .map { m =>
              val cate = m._1

              val size = m._2.size
              val score = m._2.map { mm =>
                mm._2
              }.sum
              Term(cate, score / size)
            }.toSeq
          ss
        }.toSeq

        val kwq = value_bts.filter(f => (!f.text.isEmpty && f.text != None))
          .flatMap { mm =>
            val query = mm.text.trim.replace(" ", "")
            val term1 = qtw.getWeight(query).asScala.map { t =>
              if (t.split("\t").size == 2) {
                val key = t.split("\t")(0).trim
                val value = t.split("\t")(1).trim.toDouble
                Term(key, value)
              } else {
                Term("###", 0.000)
              }
            }
            val term2 = gtm.getTermImp(query).asScala.map { t =>
              if (t.split("\t").size == 2) {
                val key = t.split("\t")(0).trim
                val value = t.split("\\t")(1).trim.toDouble
                Term(key, value)
              } else {
                Term("###", 0.000)
              }
            }
            val nterm1 = term1.filter(f => f.cate != "###").toList
            val nterm2 = term2.filter(f => f.cate != "###").toList
            val term = nterm1 ++ nterm2
            term
          }.toList

        //添加 app extension category; emicategory; lda topic; keywords
        //添加 app extension category;
        val appGoogleCat = value_bts.flatMap{rows=>
          val sourceId = rows.sourceId
          val actionId = rows.actionId
          val re1 = if(sourceId == 3){
            val packageName = rows.entityKey.split('|').last
            if(appCategoryMapPackageBC.value.contains(packageName))
            {
              appCategoryMapPackageBC.value.get(packageName).get.map(r=>(r._1, r._3))
            }
            else Seq()
          }
          else if(sourceId == 5 && (actionId == 1|| actionId == 4)){
            val appid = rows.text
            if(appCategoryMapAppIdBC.value.contains(appid))
            {
              appCategoryMapAppIdBC.value.get(appid).get.map(r=>(r._1, r._3))
            }
            else Seq()
          }
          else Seq()
          re1
        }.toSeq
          .groupBy(f=>f._1)
          .map{row=>
            val catId = row._1
            val size = row._2.size
            val score = row._2.map{r=>r._2}.sum
            Term(catId.toString, score/size)
          }.toSeq

        val appEmiCat = value_bts
          .flatMap{rows=>
            val sourceId = rows.sourceId
            val actionId = rows.actionId
            val re1 = if(sourceId == 3){
              val packageName = rows.entityKey.split('|').last
              if(appEmiCategoryMapPackageBC.value.contains(packageName)) appEmiCategoryMapPackageBC.value.get(packageName).get
              else Seq()
            }
            else if(sourceId == 5 && (actionId == 1 || actionId == 4)){
              val appId = rows.text
              if(appEmiCategoryMapAppIdBC.value.contains(appId)) appEmiCategoryMapAppIdBC.value.get(appId).get
              else Seq()
            }
            else Seq()
            re1
          }.toSeq
          .groupBy(f=>f._1)
          .map{row=>
            val catName = row._1
            val size = row._2.size
            val score = row._2.map{r=>r._2}.sum
            Term(catName, score/size)
          }.toSeq

        //添加 app extension lda topic;
        val appLdaTopic = value_bts
          .flatMap{rows=>
            val sourceId = rows.sourceId
            val actionId = rows.actionId
            val re1 = if(sourceId == 3){
              val packageName = rows.entityKey.split('|').last
              if(appLdaTopicMapPackageBC.value.contains(packageName)) appLdaTopicMapPackageBC.value.get(packageName).get.map(r=>(r._1, r._2))
              else Seq()
            }
            else if(sourceId == 5&& (actionId==1||actionId==4)){
              val appid = rows.text
              if(appLdaTopicMapAppIdBC.value.contains(appid)) appLdaTopicMapAppIdBC.value.get(appid).get.map(r=>(r._1, r._2))
              else Seq()
            }
            else Seq()
            re1
          }.toSeq.groupBy(f=>f._1)
          .map{row=>
            val catName = row._1.toString
            val size = row._2.size
            val score = row._2.map{r=>r._2}.sum
            Term(catName, score/size)
          }.toSeq
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
        val appKeyWords = if(appKeywords==null||appKeywords.isEmpty) Seq() else appKeywords
        ResultAppExtension(imei, google, topic, emi, kwq, appGoogleCat, appLdaTopic, appEmiCat, appKeyWords)
//        ResultAppExtension(imei, google, topic, emi, kwq, appGoogleCat, Seq(), Seq(), Seq())
      }
    tt.show(false)
    tt.filter(r => r.gCatSeq.nonEmpty || r.emiCatSeq.nonEmpty || r.topicSeq.nonEmpty||r.appEmiCatSeq.nonEmpty||r.appGoogleCatSeq.nonEmpty||r.appTopicSeq.nonEmpty||r.appKeyWordsSeq.nonEmpty)
      .repartition(100)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(args("output"))
    println("Task Finished")
    spark.stop()
  }
}
