package com.xiaomi.ad.keyword.marchinggiant.user

import com.twitter.scalding.Args
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

/**
  * Created by cailiming on 18-2-1.
  */
object AppDownloadMappingJob {
    def main(args: Array[String]): Unit = {
        val argv = Args(args)
        val sparkConf = new SparkConf()
        execute(argv, sparkConf)
    }

    def execute(args: Args, sparkConf: SparkConf) = {
        val spark = SparkSession
            .builder()
            .config(sparkConf)
            .getOrCreate()

        import spark.implicits._

        val appInfo = spark.read.text(args("app")).as[String]
            .map { line =>
                val split = line.split("\t", 5)
                val pn = split.head
                val appId = split(2).toLong

                val lv1 = split(3).toInt
                val lv2 = split(4).toInt

                pn -> (appId, lv1, lv2)
            }
            .collect()
            .toMap

        val appInfoBroadcast = spark.sparkContext.broadcast(appInfo)

        val rawDF = spark.read.text(args("pair"))
            .as[String]
            .filter { line =>
                val split = line.split("\t", 3)

                val pn1 = split.head
                val pn2 = split(1)
                val cnt = split.last.toInt

                pn1 != pn2 && cnt >= 3 && appInfoBroadcast.value.contains(pn1) && appInfoBroadcast.value.contains(pn2)
            }
            .map { line =>
                val split = line.split("\t", 3)

                val pn1 = split.head
                val pn2 = split(1)
                val cnt = split.last.toInt

                val app1 = appInfoBroadcast.value(pn1)
                val app2 = appInfoBroadcast.value(pn2)

                (app1, app2, cnt)
            }.cache()

        //app->app
        rawDF
            .map { case(app1, app2, cnt) =>
                app1._1 + "\t" + app2._1 + "\t" + Math.floor(Math.log(cnt))
            }
            .repartition(1)
            .write
            .mode(SaveMode.Overwrite)
            .text(args("output") + "/app2app")

        //app->cate
        val app2CateDF = rawDF
            .map { case(app1, app2, cnt) =>
                (app1._1 + "\t" + app2._2 + "_" + app2._3, cnt)
            }
            .groupBy($"_1")
            .agg(sum($"_2").alias("cnt"))
            .select($"_1".alias("key"), $"cnt")
            .cache()

        val cateCntMap = app2CateDF
            .map { row =>
                val key = row.getAs[String]("key").split("\t").head
                val cnt = row.getAs[Long]("cnt")

                key -> cnt
            }
            .groupBy($"_1")
            .agg(sum($"_2").alias("cnt"))
            .map { r =>
                val k = r.getAs[String]("_1")
                val cnt = r.getAs[Long]("cnt")

                k -> cnt
            }
            .collect()
            .toMap

        val cateCntMapBroadcast = spark.sparkContext.broadcast(cateCntMap)

        app2CateDF
            .map { row =>
                val key = row.getAs[String]("key")
                val cnt = row.getAs[Long]("cnt")

                val keySplit = key.split("\t", 2)
                val app = keySplit.head
                val cate = keySplit.last

                val score = cnt * 10.0 / cateCntMapBroadcast.value(app)

                f"$app\t$cate\t$score%1.4f"
            }
            .repartition(1)
            .write
            .mode(SaveMode.Overwrite)
            .text(args("output") + "/app2category")

        app2CateDF.unpersist()

        //cate2cate
        val cate2CateDF = rawDF
            .map { case(app1, app2, cnt) =>
                (app1._2 + "_" + app1._3 + "\t" + app2._2 + "_" + app2._3, cnt)
            }
            .groupBy($"_1")
            .agg(sum($"_2").alias("cnt"))
            .select($"_1".alias("key"), $"cnt")
            .cache()

        val cate2CatCntMap = cate2CateDF
            .map { row =>
                val key = row.getAs[String]("key").split("\t").head
                val cnt = row.getAs[Long]("cnt")

                key -> cnt
            }
            .groupBy($"_1")
            .agg(sum($"_2").alias("cnt"))
            .map { r =>
                val k = r.getAs[String]("_1")
                val cnt = r.getAs[Long]("cnt")

                k -> cnt
            }
            .collect()
            .toMap
        val cate2CateCntMapBroadcast = spark.sparkContext.broadcast(cate2CatCntMap)

        cate2CateDF
            .map { row =>
                val key = row.getAs[String]("key")
                val cnt = row.getAs[Long]("cnt")

                val keySplit = key.split("\t", 2)
                val cate1 = keySplit.head
                val cate2 = keySplit.last

                val score = cnt * 10.0 / cate2CateCntMapBroadcast.value(cate1)

                f"$cate1\t$cate2\t$score%1.4f"
            }
            .repartition(1)
            .write
            .mode(SaveMode.Overwrite)
            .text(args("output") + "/category2category")

        cate2CateDF.unpersist()

        //cate2app
        val cate2appDF = rawDF
            .map { case(app1, app2, cnt) =>
                (app1._2 + "_" + app1._3 + "\t" + app2._1, cnt)
            }
            .groupBy($"_1")
            .agg(sum($"_2").alias("cnt"))
            .select($"_1".alias("key"), $"cnt")
            .cache()

        val cate2appCntMap = cate2appDF
            .map { row =>
                val key = row.getAs[String]("key").split("\t").head
                val cnt = row.getAs[Long]("cnt")

                key -> cnt
            }
            .groupBy($"_1")
            .agg(sum($"_2").alias("cnt"))
            .map { r =>
                val k = r.getAs[String]("_1")
                val cnt = r.getAs[Long]("cnt")

                k -> cnt
            }
            .collect()
            .toMap
        val cate2appCntMapBroadcast = spark.sparkContext.broadcast(cate2appCntMap)

        cate2appDF
            .map { row =>
                val key = row.getAs[String]("key")
                val cnt = row.getAs[Long]("cnt")

                val keySplit = key.split("\t", 2)
                val cate1 = keySplit.head
                val app = keySplit.last

                val score = cnt * 10.0 / cate2appCntMapBroadcast.value(cate1)

                f"$cate1\t$app\t$score%1.4f"
            }
            .repartition(1)
            .write
            .mode(SaveMode.Overwrite)
            .text(args("output") + "/category2app")

        cate2appDF.unpersist()

        //lv1Cate2lv1Cate
        val lv1Cate2Lv1Cate = rawDF
            .map { case(app1, app2, cnt) =>
                (app1._2 + "\t" + app2._2, cnt)
            }
            .groupBy($"_1")
            .agg(sum($"_2").alias("cnt"))
            .select($"_1".alias("key"), $"cnt")
            .cache()

        val lv1CateCntMap = lv1Cate2Lv1Cate
            .map { row =>
                val key = row.getAs[String]("key").split("\t").head
                val cnt = row.getAs[Long]("cnt")

                key -> cnt
            }
            .groupBy($"_1")
            .agg(sum($"_2").alias("cnt"))
            .map { r =>
                val k = r.getAs[String]("_1")
                val cnt = r.getAs[Long]("cnt")

                k -> cnt
            }
            .collect()
            .toMap
        val lv1CateCntMapBroadcast = spark.sparkContext.broadcast(lv1CateCntMap)

        lv1Cate2Lv1Cate
            .map { row =>
                val key = row.getAs[String]("key")
                val cnt = row.getAs[Long]("cnt")

                val keySplit = key.split("\t", 2)
                val cate1 = keySplit.head
                val cate2 = keySplit.last

                val score = cnt * 10.0 / lv1CateCntMapBroadcast.value(cate1)

                f"$cate1\t$cate2\t$score%1.4f"
            }
            .repartition(1)
            .write
            .mode(SaveMode.Overwrite)
            .text(args("output") + "/lv1category2lv1category")

        lv1Cate2Lv1Cate.unpersist()

        rawDF.unpersist()
    }
}
