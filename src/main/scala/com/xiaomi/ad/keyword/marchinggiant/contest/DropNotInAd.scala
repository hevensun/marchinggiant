package com.xiaomi.ad.keyword.marchinggiant.contest

import com.twitter.scalding.Args
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by cailiming on 17-9-8.
  */
object DropNotInAd {
    val AD_PATH="develop/cailiming/tmp/contest-hashed/ad_info"
    val LABEL_PATH="develop/cailiming/tmp/contest-hashed/contest_dataset_label"
    val TEST_PATH="develop/cailiming/tmp/contest-hashed/contest_testset"
    val TEST_WITH_LABEL_PATH="develop/cailiming/tmp/contest-hashed/with_label"

    val OUTPUT = "develop/cailiming/tmp1/contest-hashed/"

    def main(args: Array[String]): Unit = {
        val argv = Args(args)
        execute(argv, new SparkConf())
    }

    def execute(args: Args, sparkConf: SparkConf) = {
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._

        val allAdBroadCast = spark.sparkContext.broadcast(spark.read.text(AD_PATH).as[String]
            .map{ l =>
                val splits = l.split("\t")
                splits(2)
            }
            .collect()
            .toSet
        )

        val connectionTypeSet = spark.sparkContext.broadcast(Map(
            "BLUETOOTH TETHERING" -> "\\N",
            "MOBILE_HIPRI" -> "MOBILE",
            "VPN" -> "\\N",
            "MOBILE_SUPL" -> "MOBILE",
            "ETHERNET" -> "\\N",
            "MOBILE_MMS" -> "MOBILE",
            "BLUETOOTH_TETHER" -> "\\N",
            "MOBILE" -> "MOBILE",
            "WIFI" -> "WIFI"
        ))

//        (11 to 30).foreach{ curDate =>
//            spark.read.text(LABEL_PATH + s"/date=$curDate")
//                .as[String]
//                .filter{ l =>
//                    val splits = l.split("\categoryName.txt", 10)
//                    allAdBroadCast.value.contains(splits(3))
//                }
//                .map{ l =>
//                    val splits = l.split("\categoryName.txt", 10)
//                    val connectionType = if (splits(6).trim.isEmpty || splits(6).trim == "\\N") "\\N" else connectionTypeSet.value(splits(6).trim.toUpperCase())
//
//                    s"${splits.head}\categoryName.txt${splits(1)}\categoryName.txt${splits(2)}\categoryName.txt${splits(3)}\categoryName.txt${splits(4)}\categoryName.txt${splits(5)}\categoryName.txt$connectionType\categoryName.txt${splits(7)}\categoryName.txt${splits(8)}\categoryName.txt${splits(9)}"
//                }
//                .write
//                .mode(SaveMode.Overwrite)
//                .text(OUTPUT + s"contest_dataset_label/date=$curDate")
//        }

        (32 to 33).foreach{ curDate =>
//            spark.read.text(TEST_PATH + s"/date=$curDate")
//                .as[String]
//                .filter{ l =>
//                    val splits = l.split("\categoryName.txt")
//                    allAdBroadCast.value.contains(splits(3))
//                }
//                .map{ l =>
//                    val splits = l.split("\categoryName.txt", 10)
//                    val connectionType = if (splits(6).trim.isEmpty || splits(6).trim == "\\N") "\\N" else connectionTypeSet.value(splits(6).trim.toUpperCase())
//
//                    s"${splits.head}\categoryName.txt${splits(1)}\categoryName.txt${splits(2)}\categoryName.txt${splits(3)}\categoryName.txt${splits(4)}\categoryName.txt${splits(5)}\categoryName.txt$connectionType\categoryName.txt${splits(7)}\categoryName.txt${splits(8)}\categoryName.txt${splits(9)}"
//                }
//                .write
//                .mode(SaveMode.Overwrite)
//                .text(OUTPUT + s"contest_testset/date=$curDate")

            spark.read.text(TEST_WITH_LABEL_PATH + s"/date=$curDate")
                .as[String]
                .filter{ l =>
                    val splits = l.split("\t")
                    allAdBroadCast.value.contains(splits(3))
                }
                .map{ l =>
                    val splits = l.split("\t", 10)
                    val connectionType = if (splits(6).trim.isEmpty || splits(6).trim == "\\N") "\\N" else connectionTypeSet.value(splits(6).trim.toUpperCase())

                    s"${splits.head}\t${splits(1)}\t${splits(2)}\t${splits(3)}\t${splits(4)}\t${splits(5)}\t$connectionType\t${splits(7)}\t${splits(8)}\t${splits(9)}"
                }
                .write
                .mode(SaveMode.Overwrite)
                .text(OUTPUT + s"with_label/date=$curDate")
        }
    }
}
