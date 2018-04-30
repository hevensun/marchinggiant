package com.xiaomi.ad.keyword.marchinggiant.contest

import java.io.{BufferedWriter, OutputStreamWriter}

import com.twitter.scalding.Args
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by cailiming on 17-9-7.
  */
object HashData {
    val LABEL_PATH="develop/cailiming/tmp/contest_dataset_label"
    val TEST_PATH="develop/cailiming/tmp/contest_dataset_test/date={20170815,20170816}"
    val AD_PATH="develop/cailiming/tmp/contest_dataset_ad"
    val PROFILE_PATH="develop/cailiming/tmp/contest_dataset_user_profile/20170715"

    val RESULT_MAP_PATH="develop/cailiming/tmp/id-map/"

    def main(args: Array[String]): Unit = {
        val argv = Args(args)
        execute(argv, new SparkConf())
    }

    def execute(args: Args, sparkConf: SparkConf) = {
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._

        val advertiserMap = getAdvertiserIdMap(spark)
        writeMap(RESULT_MAP_PATH + "advertiser", advertiserMap)

        val compaignIdMap = getCompaignIdMap(spark)
        writeMap(RESULT_MAP_PATH + "compaign", compaignIdMap)

        val adIdMap = getAdIdMap(spark)
        writeMap(RESULT_MAP_PATH + "ad", adIdMap)

        val positionMap = getPositionIdMap(spark)
        writeMap(RESULT_MAP_PATH + "position", positionMap)

        val ipMap = getIPMap(spark)
        writeMap(RESULT_MAP_PATH + "ip", ipMap)

        val deviceInfoMap = getDeviceInfoMap(spark)
        writeMap(RESULT_MAP_PATH + "deviceInfo", deviceInfoMap)

        val provinceMap = getProvinceMap(spark)
        writeMap(RESULT_MAP_PATH + "province", provinceMap)

        val cityMap = getCityMap(spark)
        writeMap(RESULT_MAP_PATH + "city", cityMap)

        val miuiMap = getMiuiVersionMap(spark)
        writeMap(RESULT_MAP_PATH + "miuiVersion", miuiMap)

        val androidMap = getAndroidVersionMap(spark)
        writeMap(RESULT_MAP_PATH + "androidVersion", androidMap)

    }

    def writeMap(file: String, ans: Seq[(String, Int)]) = {
        val result = ans.map{ case(s, i) => s + "\t" + i}
        writeFile(file, result)
    }

    def writeFile(file: String, ans: Seq[String]) = {
        val fs = FileSystem.get(new Configuration())
        val path = new Path(file)
        val bw = new BufferedWriter(new OutputStreamWriter(fs.create(path), "UTF-8"))

        ans.foreach(l => bw.write(l + "\n"))

        bw.flush()
        bw.close()
    }

    def getProvinceMap(spark: SparkSession) = {
        import spark.implicits._
        val advertiser = spark.read.text(PROFILE_PATH)
            .as[String]
            .map{ l =>
                val splits = l.split("\t", 8)
                splits(4).trim
            }

        advertiser
            .distinct()
            .filter{ r =>
                r.trim.nonEmpty && r.trim != "\\N"
            }
            .repartition(10000)
            .collect()
            .zipWithIndex
            .toSeq
    }

    def getCityMap(spark: SparkSession) = {
        import spark.implicits._
        val advertiser = spark.read.text(PROFILE_PATH)
            .as[String]
            .map{ l =>
                val splits = l.split("\t", 8)
                splits(5).trim
            }

        advertiser
            .distinct()
            .filter{ r =>
                r.trim.nonEmpty && r.trim != "\\N"
            }
            .repartition(10000)
            .collect()
            .zipWithIndex
            .toSeq
    }

    def getDeviceInfoMap(spark: SparkSession) = {
        import spark.implicits._
        val advertiser = spark.read.text(PROFILE_PATH)
            .as[String]
            .map{ l =>
                val splits = l.split("\t", 8)
                splits(6).trim
            }

        advertiser
            .distinct()
            .filter{ r =>
                r.trim.nonEmpty && r.trim != "\\N"
            }
            .repartition(10000)
            .collect()
            .zipWithIndex
            .toSeq
    }

    def getMiuiVersionMap(spark: SparkSession) = {
        import spark.implicits._
        val labelIp = spark.read.text(LABEL_PATH)
            .as[String]
            .map{ l =>
                val splits = l.split("\t", 10)
                splits(7).trim
            }

        val testIp = spark.read.text(TEST_PATH)
            .as[String]
            .map{ l =>
                val splits = l.split("\t", 10)
                splits(7).trim
            }

        labelIp.union(testIp)
            .distinct()
            .filter{ r =>
                r.trim.nonEmpty && r.trim != "\\N"
            }
            .repartition(10000)
            .collect()
            .zipWithIndex
            .toSeq
    }

    def getAndroidVersionMap(spark: SparkSession) = {
        import spark.implicits._
        val labelIp = spark.read.text(LABEL_PATH)
            .as[String]
            .map{ l =>
                val splits = l.split("\t", 10)
                splits(9).trim
            }

        val testIp = spark.read.text(TEST_PATH)
            .as[String]
            .map{ l =>
                val splits = l.split("\t", 10)
                splits(9).trim
            }

        labelIp.union(testIp)
            .distinct()
            .filter{ r =>
                r.trim.nonEmpty && r.trim != "\\N" && r.trim != "unknown"
            }
            .repartition(10000)
            .collect()
            .zipWithIndex
            .toSeq
    }

    def getPositionIdMap(spark: SparkSession) = {
        import spark.implicits._
        val labelIp = spark.read.text(LABEL_PATH)
            .as[String]
            .map{ l =>
                val splits = l.split("\t", 10)
                splits(5).trim
            }

        val testIp = spark.read.text(TEST_PATH)
            .as[String]
            .map{ l =>
                val splits = l.split("\t", 10)
                splits(5).trim
            }

        labelIp.union(testIp)
            .distinct()
            .filter{ r =>
                r.trim.nonEmpty && r.trim != "\\N"
            }
            .repartition(10000)
            .collect()
            .zipWithIndex
            .toSeq
    }

    def getAdIdMap(spark: SparkSession) = {
        import spark.implicits._
        val advertiser = spark.read.text(AD_PATH)
            .as[String]
            .map{ l =>
                val splits = l.split("\t")
                splits(2).trim
            }

        advertiser
            .distinct()
            .filter{ r =>
                r.trim.nonEmpty && r.trim != "\\N"
            }
            .repartition(10000)
            .collect()
            .zipWithIndex
            .toSeq
    }

    def getCompaignIdMap(spark: SparkSession) = {
        import spark.implicits._
        val advertiser = spark.read.text(AD_PATH)
            .as[String]
            .map{ l =>
                val splits = l.split("\t")
                splits(1).trim
            }

        advertiser
            .distinct()
            .filter{ r =>
                r.trim.nonEmpty && r.trim != "\\N"
            }
            .repartition(10000)
            .collect()
            .zipWithIndex
            .toSeq
    }

    def getAdvertiserIdMap(spark: SparkSession) = {
        import spark.implicits._
        val advertiser = spark.read.text(AD_PATH)
            .as[String]
            .map{ l =>
                val splits = l.split("\t")
                splits.head.trim
            }

        advertiser
            .distinct()
            .filter{ r =>
                r.trim.nonEmpty && r.trim != "\\N"
            }
            .repartition(10000)
            .collect()
            .zipWithIndex
            .toSeq
    }

    def getIPMap(spark: SparkSession) = {
        import spark.implicits._
        val labelIp = spark.read.text(LABEL_PATH)
            .as[String]
            .map{ l =>
                val splits = l.split("\t", 10)
                splits(8).trim
            }

        val testIp = spark.read.text(TEST_PATH)
            .as[String]
            .map{ l =>
                val splits = l.split("\t", 10)
                splits(8).trim
            }

        labelIp.union(testIp)
            .distinct()
            .filter{ r =>
                r.trim.nonEmpty && r.trim != "\\N"
            }
            .repartition(10000)
            .collect()
            .zipWithIndex
            .toSeq
    }
}
