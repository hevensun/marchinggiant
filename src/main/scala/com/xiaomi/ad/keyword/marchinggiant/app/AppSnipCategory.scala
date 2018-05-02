package com.xiaomi.ad.keyword.marchinggiant.app

import com.twitter.scalding.Args
import com.xiaomi.matrix.cat.{CatModelConf, CatModelFacade}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.json.JSONObject

/**
  * Created by cailiming on 18-2-5.
  */
object AppSnipCategory {
    val CATEGORY_NUM = 10

    def main(args: Array[String]): Unit = {
        val argv = Args(args)
        execute(argv, new SparkConf())
    }

    def execute(args: Args, sparkConf: SparkConf) = {
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._

        val googleConf = CatModelConf.defaultGoogleConf
        val classifyModelData = spark
            .sparkContext
            .broadcast(CatModelFacade.getOrInit(spark.sparkContext, googleConf).modelData)

        val appInfo = spark.read.parquet(args("app-info"))
            .map { row =>
                val appId = row.getAs[Long]("appId")
                val displayName = row.getAs[String]("displayName")

                appId -> displayName.trim
            }
            .collect()
            .filter(_._1 > 0)
            .toSeq
            .groupBy(_._2)
            .mapValues { vs =>
                vs.map(_._1)
            }

        val appInfoBroadcast = spark.sparkContext.broadcast(appInfo)

        val appNameCateDF = spark.read.parquet(args("app-snippets"))
            .as[AppSnippets]
            .repartition(20)
            .mapPartitions { pts =>
                val model = CatModelFacade.getOrInit(googleConf, classifyModelData.value)

                pts.map { as =>
                    val appName = as.query.substring(0, as.query.length - 4)

                    val resultStr = as.searchResult.filter(ts => ts.title.contains(appName) || ts.summary.contains(appName))
                        .map(ts => s"${ts.title} ${ts.summary}").mkString(" ")

                    val searchResultCategory = model
                        .predict(resultStr, CATEGORY_NUM)
                        .map(r => Category(r.id.toInt, r.name, r.score))
                        .toSeq

                    AppSnippetsWithCategory(as.query, resultStr, searchResultCategory)
                }
            }
            .cache()

        val appIdCateDF = appNameCateDF
            .flatMap { appSC =>
                val appName = appSC.query.substring(0, appSC.query.length - 4)

                val appIds = appInfoBroadcast.value.getOrElse(appName, Seq())
                appIds
                    .map(id => AppId2Category(id, appSC.googleCategories))
            }

        appNameCateDF
            .repartition(1)
            .write
            .mode(SaveMode.Overwrite)
            .parquet(args("appNameCate"))

        appIdCateDF
            .repartition(1)
            .write
            .mode(SaveMode.Overwrite)
            .parquet(args("appIdCate"))

        appNameCateDF.unpersist()

        spark.stop()
    }
}
