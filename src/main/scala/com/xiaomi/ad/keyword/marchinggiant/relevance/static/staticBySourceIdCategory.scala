package com.xiaomi.ad.keyword.marchinggiant.relevance.static

/**
  * create by liguoyu on 2018-05-08
  */

import com.twitter.scalding.Args
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

object staticBySourceIdCategory {
  def main(args: Array[String]): Unit = {
    val input_params = Args(args);
    val conf = new SparkConf()
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    // static
    val filteUdf = udf((sourceId: Int, actionId: Int) =>{
      sourceId == 3 || (sourceId == 5 && (actionId == 1 || actionId == 4))
    })
    val data1 = spark.read.parquet(input_params("input_data1"))
      .filter($"sourceId"===3||($"sourceId"===5&&($"actionId"===1||$"actionId"===4)))
      .select($"imei1Md5", $"extension")
    val data2 = spark.read.parquet(input_params("input_data2"))
      .filter($"sourceId"===3||($"sourceId"===5&&($"actionId"===1||$"actionId"===4)))
      .select($"imei1Md5", $"extension")
    val extensiongoogleudf = udf((extension: Map[String, String])=>{
      extension.nonEmpty&&extension.contains("2")
    })
    val extensionldaudf = udf((extension: Map[Int, String])=>{
      extension.nonEmpty&&extension.contains(3)
    })

    val extensionEmiudf = udf((extension: Map[Int, String])=>{
      extension.nonEmpty&&extension.contains(6)
    })

    val static_cate_uv_1 = data1.filter(extensiongoogleudf($"extension")).count()
    val static_cate_uv_2 = data2.filter(extensiongoogleudf($"extension")).count()

    val static_emiCaet_uv_1 = data1.filter(extensionEmiudf($"extension")).count()
    val static_emiCaet_uv_2 = data2.filter(extensionEmiudf($"extension")).count()

    val static_lda_uv_1 = data1.filter(extensionldaudf($"extension")).count()
    val static_lda_uv_2 = data2.filter(extensionldaudf($"extension")).count()

    println("--------------------------------------------------------------------------------")
    println("google cate UV compare:",static_cate_uv_1,"-----", static_cate_uv_2)
    println("emi cate UV compare:", static_emiCaet_uv_1, "-----", static_emiCaet_uv_2)
    println("lda topic UV compare:", static_lda_uv_1, "------", static_lda_uv_2)
    println("--------------------------------------------------------------------------------")
    spark.stop()
  }
}
