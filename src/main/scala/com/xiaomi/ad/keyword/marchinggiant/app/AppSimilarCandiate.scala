package com.xiaomi.ad.keyword.marchinggiant.app

import com.twitter.scalding.Args
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

/**
  * Created by cailiming on 17-12-11.
  */
object AppSimilarCandiate {
    def main(args: Array[String]): Unit = {
        val argv = Args(args)
        execute(argv, new SparkConf())
    }

    def execute(args: Args, sparkConf: SparkConf) = {
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._

        val nameEqualUdf = udf { (app1: String, app2: String) =>
            app1 != app2
        }

        val minUdf = udf { (app1: String, app2: String) =>
            if (app1.compareTo(app2) < 0) app1 else app2
        }

        val maxUdf = udf { (app1: String, app2: String) =>
            if (app1.compareTo(app2) >= 0) app1 else app2
        }

        def categoryInUdf(categories: Seq[String]) = {
            udf { c: String =>
                c != null && categories.contains(c)
            }
        }

        val categorySets = Seq(
            Seq("游戏"),
            Seq("实用工具", "学习教育", "金融理财", "居家生活", "聊天社交", "时尚购物", "效率办公", "旅行交通", "影音视听", "新闻资讯", "医疗健康", "娱乐消遣", "摄影摄像", "图书阅读", "体育运动"),
            Seq("学习教育", "效率办公", "图书阅读", "金融理财", "聊天社交"),
            Seq("金融理财", "居家生活", "时尚购物"),
            Seq("居家生活", "学习教育", "聊天社交", "时尚购物", "旅行交通", "医疗健康", "体育运动", "新闻资讯"),
            Seq("聊天社交", "影音视听", "旅行交通", "娱乐消遣", "摄影摄像", "新闻资讯"),
            Seq("时尚购物", "旅行交通", "娱乐消遣"),

            Seq("效率办公", "旅行交通", "图书阅读"),
            Seq("旅行交通", "学习教育", "新闻资讯"),
            Seq("影音视听", "摄影摄像", "娱乐消遣"),
            Seq("新闻资讯"),
            Seq("医疗健康"),
            Seq("娱乐消遣"),
            Seq("摄影摄像"),
            Seq("图书阅读"),
            Seq("体育运动")
        )

        val appInfo = spark.read.parquet(args("appInfo"))
            .filter($"appId" =!= 0)
            .repartition(100)

        val appPair = categorySets
            .map { categories =>
                val leftCategory = spark.sparkContext.broadcast(Seq(categories.head))
                val rightCategory = spark.sparkContext.broadcast(categories)

                val appInfo1 = appInfo
                    .filter(categoryInUdf(leftCategory.value)($"level1CategoryName"))
                    .select($"packageName".alias("packageName1"), lit("c").alias("tmpField"))

                val appInfo2 = appInfo
                    .filter(categoryInUdf(rightCategory.value)($"level1CategoryName"))
                    .select($"packageName".alias("packageName2"), lit("c").alias("tmpField"))

                appInfo1
                    .join(appInfo2, Seq("tmpField"))
                    .filter(nameEqualUdf($"packageName1", $"packageName2"))
                    .select(minUdf($"packageName1", $"packageName2").alias("app1"), maxUdf($"packageName1", $"packageName2").alias("app2"), lit("0").alias("label"))
                    .distinct()
                    .as[AppSimilarPair]
            }
            .reduce { (a: Dataset[AppSimilarPair], b: Dataset[AppSimilarPair]) =>
                a.union(b)
            }
            .distinct()

        appPair
            .write
            .mode(SaveMode.Overwrite)
            .format("parquet")
            .save(args("output"))

        spark.stop()
    }
}
