package com.xiaomi.ad.keyword.marchinggiant.relevance.infofeed

/**
  */

import com.twitter.scalding.Args
import com.xiaomi.ad.qu.get_query_term_weight.usage.{getTermImp, getTermWeight}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.JavaConverters._


object userKeywordsVec {

    val N = 3
    case class Term(cate: String, score: Double)
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

    case class UserKWs(imei: String, kwSeq: Seq[Term])

    def main(args: Array[String]): Unit = {
        val argv = Args(args)
        execute(argv, new SparkConf())
    }

    def shortMustChinese(query: String) = {
        if (query.trim.length > 1) {
            query != null && query.map(c => c >= 0x4e00 && c <= 0x9fbb).reduce(_ && _)
        } else {
            false
        }
    }

    def iskeywords(word: String, nature: String): Boolean = {
        shortMustChinese(word) && ((nature.startsWith("n") && nature != "null") || nature.startsWith("v"))
    }

    def execute(args: Args, sparkConf: SparkConf) = {
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._

        val gtm = new getTermImp()

        val matrix = spark.read.parquet(args("input_matrix"))
            .as[BehaviorTag]
            .filter(f => (f.sourceId == 1))
        matrix.persist()
        val threshold = args("input_threshold").toInt


        val userFilter = matrix.map { m =>
            m.imei1Md5 -> 1
        }.rdd
            .reduceByKey(_ + _)
            .sortBy(s => s._2, false)
            .filter(f => f._2 > threshold)
            .map { m =>
                m._1
            }.collect().toSeq

        val userFilterB = spark.sparkContext.broadcast(userFilter)

        val tt = matrix.filter(f => !userFilterB.value.contains(f.imei1Md5))
            .groupByKey(_.imei1Md5)
            .mapGroups { case (imei, bts) =>
                val kwq = bts.filter(f => !f.text.isEmpty && f.text != None)
                    .flatMap { mm =>
                        val query = mm.text.trim.replace(" ", "")
                        val termSeq = gtm.getTermWeighting2NatureTerm(query).asScala
                            .sortBy(_.getWeight).reverse
                            .take(N)
                            .filter(t => iskeywords(t.getName, t.qtwNatureStr()))
                            .map { t => Term(t.getName, t.getWeight) }
                        termSeq
                    }.toSet.toSeq
                UserKWs(imei, kwq)
            }
        tt.filter(t => t.kwSeq.nonEmpty)
            .repartition(200)
            .write
            .mode(SaveMode.Overwrite)
            .parquet(args("output"))

        matrix.unpersist()
        spark.stop()
    }
}
