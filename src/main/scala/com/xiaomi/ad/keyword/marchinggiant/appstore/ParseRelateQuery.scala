package com.xiaomi.ad.keyword.marchinggiant.appstore

import com.twitter.scalding.Args
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.json4s._
import org.json4s.native.JsonMethods._

/**
  * Created by cailiming on 17-8-8.
  */
object ParseRelateQuery {
    def main(args: Array[String]): Unit = {
        val argv = Args(args)
        execute(argv, new SparkConf())
    }

    def execute(args: Args, sparkConf: SparkConf) = {
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._

        val inputDf = spark.read.text(args("input")).repartition(10)

        inputDf
            .map{ r =>
                val line = r.getAs[String]("value")
                getQueryInfo(line)
            }
            .filter(!_.query.startsWith("Error"))
            .repartition(2)
            .write
            .format("parquet")
            .mode(SaveMode.Overwrite)
            .save(args("output"))

        spark.stop()
    }

    def isValidQueryInfoKeySet(keySet: Set[String]): Boolean = {
        keySet.contains("百科") && keySet.contains("相关搜索") && keySet.contains("广告") && keySet.contains("搜索结果") && keySet.size == 4
    }

    def createErrorQI(errorType: String, queryStr: String): RelateQueryInfo = {
        RelateQueryInfo(errorType + queryStr)
    }

    def getQueryInfo(queryStr: String): RelateQueryInfo = {
        try{
            val json_JObject = parse(queryStr).asInstanceOf[JObject]

            // 确认当前 json 串里面 只有一个 query
            var query = ""
            val keys = json_JObject.values.keySet
            if(keys.size > 1){
                createErrorQI("Error-query: ", queryStr)
            }else{
                query = keys.head
            }

            // 确认当前 json 串里面的信息 有且仅有: 百科, 相关搜索, 广告, 搜索结果
            val queryInfo = (json_JObject \ query).asInstanceOf[JObject]
            val queryInfoKeySet = queryInfo.values.keySet
            if(!isValidQueryInfoKeySet(queryInfoKeySet)){
                createErrorQI("Error-WrongKeySet: ", queryStr)
            }

            // baike
            val baike = (queryInfo \ "百科").asInstanceOf[JString].values

            RelateQueryInfo(query, baike, getSeqFromJson(queryInfo, "相关搜索"), getTextInfoFromJson(queryInfo, "广告"), getTextInfoFromJson(queryInfo, "搜索结果"))

        }catch{
            case _: Exception => createErrorQI("Error: ", queryStr)
        }

    }

    def getSeqFromJson(queryInfo: JObject, key: String): scala.collection.immutable.Seq[String] = {
        (queryInfo \ key).asInstanceOf[JArray]
            .values
            .map(x => x.toString)
    }

    def getTextInfoFromJson(queryInfo: JObject, key: String): scala.collection.mutable.Seq[ItemInfo] = {
        val arrVal = (queryInfo \ key).asInstanceOf[JArray].values

        var ansMap = scala.collection.mutable.Map.empty[String, String]
        for(x <- arrVal){
            ansMap = ansMap ++ x.asInstanceOf[Map[String, String]] // !!! 如果 curMap size > 1, 可能有问题 ？
        }

        var ansSeq = scala.collection.mutable.Seq.empty[ItemInfo]
        for(x <- ansMap){
            ansSeq = ansSeq ++ Seq(ItemInfo(x._1, x._2))
        }
        ansSeq
    }
}
