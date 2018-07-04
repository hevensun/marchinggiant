package com.xiaomi.ad.keyword.marchinggiant.relevance

import com.twitter.scalding.Args
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import com.xiaomi.ad.qu.get_query_term_weight.usage.{getTermImp, getTermWeight}
import collection.JavaConverters._
/**
  * Created by mi on 18-4-15.
  * 获取用户的category，并进行google分类扩展
  */
object userCategory {
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
            .filter(f => ( f.sourceId == 5 && f.actionId == 3) )
            //.filter(f => (f.sourceId == 1 || f.sourceId == 3 || f.sourceId == 5 || f.sourceId == 7) && !appSetB.value.contains(f.entityKey))

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
        
        val tt = matrix.filter(f => !userFilterB.value.contains(f.imei1Md5))
            .groupByKey(_.imei1Md5)
            .mapGroups { case (imei, bts) =>
                val google = bts.flatMap { m =>
                    val ss = m.extension.getOrElse("2", "0:0").split(" ")
                        .flatMap { mm =>
                            val key = mm.split(":")(0)
                            val value = mm.split(":")(1).toDouble
                            //                            key -> value
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
                
                val topic = bts.flatMap { m =>
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
                
                val emi = bts.flatMap { m =>
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
                
                val kwq = bts.filter(f => (!f.text.isEmpty && f.text != None))
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
                    }.toSeq
                Result(imei, google, topic, emi, kwq)
            }
        
        tt.filter(r => r.gCatSeq.nonEmpty || r.emiCatSeq.nonEmpty || r.topicSeq.nonEmpty)
            .repartition(100)
            .write
            .mode(SaveMode.Overwrite)
            .parquet(args("output"))
        
        spark.stop()
    }
}
