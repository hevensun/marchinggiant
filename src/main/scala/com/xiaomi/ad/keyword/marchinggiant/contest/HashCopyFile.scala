package com.xiaomi.ad.keyword.marchinggiant.contest

import com.twitter.scalding.Args
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by cailiming on 17-9-7.
  */
object HashCopyFile {
    val LABEL_PATH="develop/cailiming/tmp/contest_dataset_label"
    val TEST_PATH="develop/cailiming/tmp/contest_dataset_test/date="
    val AD_PATH="develop/cailiming/tmp/contest_dataset_ad"
    val PROFILE_PATH="develop/cailiming/tmp/contest_dataset_user_profile/20170715"

    val IDMAP_BASH_PATH="develop/cailiming/tmp/id-map"
    val OUTPUT = "develop/cailiming/tmp/contest-hashed/"

    def main(args: Array[String]): Unit = {
        val argv = Args(args)
        execute(argv, new SparkConf())
    }

    def execute(args: Args, sparkConf: SparkConf) = {
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._

        val adMapBroadCast = spark.sparkContext.broadcast(
            getIdMap("ad", spark)
        )

        val advertiserBroadCast = spark.sparkContext.broadcast(
            getIdMap("advertiser", spark)
        )

        val compaignBroadCast = spark.sparkContext.broadcast(
            getIdMap("compaign", spark)
        )

        val deviceInfoBroadCast = spark.sparkContext.broadcast(
            getIdMap("deviceInfo", spark)
        )

        val cityBroadCast = spark.sparkContext.broadcast(
            getIdMap("city", spark)
        )

        val provinceBroadCast = spark.sparkContext.broadcast(
            getIdMap("province", spark)
        )

        val appIdBroadCast = spark.sparkContext.broadcast(
            spark.read.text("matrix/tmp/contest_mapping/appId")
                .as[String]
                .map{ l =>
                    val splits = l.split("\t")
                    splits.head -> splits(1).toInt
                }
                .collect()
                .toMap
        )

        val imeiBroadCast = spark.sparkContext.broadcast(
            spark.read.text("develop/cailiming/tmp/id-map/imei-mapping")
                .as[String]
                .map{ l =>
                    val splits = l.split("\t")
                    splits.head -> splits(1).toInt
                }
                .collect()
                .toMap
        )

//        spark.read.text(AD_PATH)
//            .as[String]
//            .filter{ l =>
//                val splits = l.split("\categoryName.txt", 4)
//                appIdBroadCast.value.contains(splits(3))
//            }
//            .map{ l =>
//                val splits = l.split("\categoryName.txt", 4)
//                val advertiserId = advertiserBroadCast.value(splits.head)
//                val compaignId = compaignBroadCast.value(splits(1))
//                val adId = adMapBroadCast.value(splits(2))
//                val appId = appIdBroadCast.value(splits(3))
//                s"$advertiserId\categoryName.txt$compaignId\categoryName.txt$adId\categoryName.txt$appId"
//            }
//            .write
//            .mode(SaveMode.Overwrite)
//            .text(OUTPUT + "ad_info")

        spark.read.text(PROFILE_PATH)
            .as[String]
            .filter{ l =>
                val splits = l.split("\t", 8)
                imeiBroadCast.value.contains(splits.head)
            }
            .map{ l =>
                val splits = l.split("\t", 8)
                val userId = imeiBroadCast.value(splits.head)

                val age = if(splits(1).trim == "0" || splits(1) == "\\N") 8 else splits(1)

                val gender = if(splits(2).trim.isEmpty || splits(2) == "\\N") 3 else splits(2)

                val education = if(splits(3).trim.isEmpty || splits(3) == "\\N") "\\N" else splits(3)

                val province = if(splits(4).trim.isEmpty || splits(4) == "\\N") -1 else provinceBroadCast.value(splits(4))

                val city = if(splits(5).trim.isEmpty || splits(5) == "\\N") -1 else cityBroadCast.value(splits(5))

                val deviceInfo = if(splits(6).trim.isEmpty || splits(6).trim == "\\N") -1 else deviceInfoBroadCast.value(splits(6))
                val appList = if(splits(7).trim.isEmpty || splits(7).trim == "\\N")
                    "\\N"
                else {
                    val t = splits(7).trim.split(",")
                        .filter(a => appIdBroadCast.value.contains(a))
                        .map(a => appIdBroadCast.value(a)).mkString(",")
                    if(t.isEmpty) "\\N" else t
                }

                s"$userId\t$age\t$gender\t$education\t$province\t$city\t$deviceInfo\t$appList"
            }
            .write
            .mode(SaveMode.Overwrite)
            .text(OUTPUT + "contest_dataset_user_profile")

        spark.stop()

    }

    def getInstanceId(instance: String) = {
        val zeroNum = 7 - instance.length
        val resultBuilder = new StringBuilder
        (0 until zeroNum)
            .foreach(a => resultBuilder.append("0"))
        resultBuilder.append(instance)
        resultBuilder.toString
    }

    def getIdMap(name: String, spark: SparkSession) = {
        import spark.implicits._

        spark.read.text(s"$IDMAP_BASH_PATH/$name")
            .as[String]
            .map{ l =>
                val splits = l.split("\t")
                splits.head -> splits(1).toInt
            }
            .collect()
            .toMap
    }
}
