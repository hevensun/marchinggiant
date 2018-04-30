package com.xiaomi.ad.keyword.marchinggiant.contest

import com.twitter.scalding.Args
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.joda.time.DateTime

/**
  * Created by cailiming on 17-9-8.
  */
object HashLabelData {
    val LABEL_PATH="develop/cailiming/tmp/contest_dataset_label"
    val TEST_PATH="develop/cailiming/tmp/contest_dataset_test/date="
    val TEST_WITH_LABEL_PATH="develop/cailiming/tmp/with_label/date="

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

        val ipBroadCast = spark.sparkContext.broadcast(
            getIdMap("ip", spark)
        )

        val positionBroadCast = spark.sparkContext.broadcast(
            getIdMap("position", spark)
        )

        val miuiVersionBroadCast = spark.sparkContext.broadcast(
            getIdMap("miuiVersion", spark)
        )

        val androidVersionBroadCast = spark.sparkContext.broadcast(
            getIdMap("androidVersion", spark)
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

        val labelDate = "20170725,20170726,20170727,20170728,20170729,20170730,20170731,20170801,20170802,20170803,20170804,20170805,20170806,20170807,20170808,20170809,20170810,20170811,20170812,20170813,20170814"
//        val labelDate = "20170801,20170802,20170803,20170804,20170805,20170806,20170807,20170808,20170809,20170810,20170811,20170812,20170813,20170814"

        var i = 11
//        var i = 18
        labelDate.split(",")
            .foreach{ curDate =>
                val dayStr = if(i < 10) "0" + i else i + ""
                spark.read.text(LABEL_PATH + s"/$curDate")
                    .as[String]
                    .filter{ l =>
                        val splits = l.split("\t", 10)

                        splits(5).trim.nonEmpty
                    }
                    .map{ l =>
                        val splits = l.split("\t", 10)
                        val instanceId = dayStr + getInstanceId(splits.head.substring(8, splits.head.length))

                        val timeStamp = splits(2).toLong
                        val hm = new DateTime(timeStamp).toString("HHmm")
                        val time = if(i < 10) "0" + i + hm else i + "" + hm

                        val adId = adMapBroadCast.value(splits(3))

                        val userId = imeiBroadCast.value(splits(4))

                        val positionId = positionBroadCast.value(splits(5))

                        val connectionType = if(splits(6).trim.isEmpty || splits(6).trim == "\\N") "\\N" else splits(6).trim.toUpperCase()

                        val miuiVersion = if(splits(7).trim.isEmpty || splits(7).trim == "\\N") -1 else miuiVersionBroadCast.value(splits(7))

                        val ip = if(splits(8).trim.isEmpty || splits(8).trim == "\\N") -1 else ipBroadCast.value(splits(8))

                        val android = if(splits(9).trim.isEmpty || splits(9).trim == "\\N" || splits(9).trim == "unknown") -1 else androidVersionBroadCast.value(splits(9))

                        s"$instanceId\t${splits(1)}\t$time\t$adId\t$userId\t$positionId\t$connectionType\t$miuiVersion\t$ip\t$android"
                    }
                    .write
                    .mode(SaveMode.Overwrite)
                    .text(OUTPUT + s"contest_dataset_label/date=$dayStr")

                i = i + 1
            }

        val testDate = "20170815,20170816"

        testDate.split(",")
            .foreach { curDate =>
                spark.read.text(TEST_PATH + curDate)
                    .as[String]
                    .filter { l =>
                        val splits = l.split("\t", 10)

                        splits(5).trim.nonEmpty
                    }
                    .map { l =>
                        val splits = l.split("\t", 10)
                        val instanceId = i + getInstanceId(splits.head.substring(8, splits.head.length))

                        val timeStamp = splits(2).toLong
                        val hm = new DateTime(timeStamp).toString("HHmm")
                        val time = i + hm

                        val adId = adMapBroadCast.value(splits(3))

                        val userId = imeiBroadCast.value(splits(4))

                        val positionId = positionBroadCast.value(splits(5))

                        val connectionType = if (splits(6).trim.isEmpty || splits(6).trim == "\\N") "\\N" else splits(6).trim.toUpperCase()

                        val miuiVersion = if (splits(7).trim.isEmpty || splits(7).trim == "\\N") -1 else miuiVersionBroadCast.value(splits(7))

                        val ip = if (splits(8).trim.isEmpty || splits(8).trim == "\\N") -1 else ipBroadCast.value(splits(8))

                        val android = if (splits(9).trim.isEmpty || splits(9).trim == "\\N" || splits(9).trim == "unknown") -1 else androidVersionBroadCast.value(splits(9))

                        s"$instanceId\t${splits(1)}\t$time\t$adId\t$userId\t$positionId\t$connectionType\t$miuiVersion\t$ip\t$android"
                    }
                    .write
                    .mode(SaveMode.Overwrite)
                    .text(OUTPUT + s"contest_testset/date=$i")

                spark.read.text(TEST_WITH_LABEL_PATH + curDate)
                    .as[String]
                    .filter { l =>
                        val splits = l.split("\t", 10)

                        splits(5).trim.nonEmpty
                    }
                    .map { l =>
                        val splits = l.split("\t", 10)
                        val instanceId = i + getInstanceId(splits.head.substring(8, splits.head.length))

                        val timeStamp = splits(2).toLong
                        val hm = new DateTime(timeStamp).toString("HHmm")
                        val time = i + hm

                        val adId = adMapBroadCast.value(splits(3))

                        val userId = imeiBroadCast.value(splits(4))

                        val positionId = positionBroadCast.value(splits(5))

                        val connectionType = if (splits(6).trim.isEmpty || splits(6).trim == "\\N") "\\N" else splits(6).trim.toUpperCase()

                        val miuiVersion = if (splits(7).trim.isEmpty || splits(7).trim == "\\N") -1 else miuiVersionBroadCast.value(splits(7))

                        val ip = if (splits(8).trim.isEmpty || splits(8).trim == "\\N") -1 else ipBroadCast.value(splits(8))

                        val android = if (splits(9).trim.isEmpty || splits(9).trim == "\\N" || splits(9).trim == "unknown") -1 else androidVersionBroadCast.value(splits(9))

                        s"$instanceId\t${splits(1)}\t$time\t$adId\t$userId\t$positionId\t$connectionType\t$miuiVersion\t$ip\t$android"
                    }
                    .write
                    .mode(SaveMode.Overwrite)
                    .text(OUTPUT + s"with_label/date=$i")

                i = i + 1
            }
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
