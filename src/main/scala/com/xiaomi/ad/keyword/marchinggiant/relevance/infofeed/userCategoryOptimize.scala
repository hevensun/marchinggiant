package com.xiaomi.ad.keyword.marchinggiant.relevance.infofeed

/**
  * Create by liguoyu 2018-05-02
  * 输入  user category 向量 和 adinfo app category 向量
  * 计算 对以上二者计算 cos 距离
  * 输出 user 和 adinfo app 的相似度
  */

import com.twitter.scalding.Args
import com.xiaomi.ad.qu.get_query_term_weight.usage.{getTermImp, getTermWeight}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import collection.JavaConverters._


object userCategoryOptimize {
  case class Term(cate: String, score: Double)

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
  def main(args: Array[String]): Unit = {
    val argv = Args(args)
    execute(argv, new SparkConf())
  }

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
      .filter(f => (f.sourceId == 1 || f.sourceId == 2) && !appSetB.value.contains(f.entityKey))

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
          imei->(r.imei1Md5, gCate, emiCate, ldaTopic, keywords)
        }
      }
      .rdd
      .reduceByKey{(x, y)=>
        val imei = x._1
        val gCate = mergeCate(x._2 ++ y._2)
        val emiCate = mergeCate(x._3 ++ y._3)
        val lda = mergeCate(x._4 ++ y._4)
        val keywords = x._5 ++ y._5

        (imei, gCate, emiCate, lda, keywords)
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
        (imei, gCate, emiCate, lda, keywords)
      }.mapPartitions{rows=>
      rows.map{r=>
        val imei = r._1
        val x = r._2
        val gCate = getTerm(x._2)
        val emiCate = getTerm(x._3)
        val lda = getTerm(x._4)
        val keywords = x._5
        Result(imei, gCate, lda, emiCate, keywords)
      }
    }.toDS().as[Result]

    tt.repartition(500)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(args("output"))
    spark.stop()
    println("Task finished")
  }
}
