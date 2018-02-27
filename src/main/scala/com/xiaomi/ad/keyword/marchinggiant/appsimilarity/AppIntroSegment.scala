package com.xiaomi.ad.keyword.marchinggiant.appsimilarity

import java.io.{BufferedWriter, FileWriter}

import com.xiaomi.ad.keyword.marchinggiant.app.EnhancedAppInfo
import com.xiaomi.ad.matrix.qu.tokenizer.impl.MiTokenizer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by cailiming on 17-7-18.
  */
object AppIntroSegment {
    def execute(args: Array[String], sparkConf: SparkConf) = {
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._

        val bw = new BufferedWriter(new FileWriter("/home/mi/Develop/data/app-meta-info/app-similarity/lab/twe/trainingDocs1.dat"))

        val emptyUdf = udf { i: String =>
            i != null && i.nonEmpty
        }

        val result = spark.read.parquet("/home/mi/Develop/data/app-meta-info/total")
            .filter(emptyUdf($"introduction"))
            .as[EnhancedAppInfo]
            .map{ app =>
                MiTokenizer.segmentAndRemoveStopWords(app.introduction)
                    .map(_.name)
                    .filter(filter)
                    .mkString(" ")
            }

        val count = result.count()
        bw.write(count + "\n")

        result
            .as[String]
            .collect
            .foreach(l => bw.write(l + "\n"))

        bw.flush()
        bw.close()
    }

    def execute1(args: Array[String], sparkConf: SparkConf) = {
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._

        val bw = new BufferedWriter(new FileWriter("/home/mi/Develop/data/app-meta-info/app-similarity/lab/twe/appName.dat"))

        val emptyUdf = udf { i: String =>
            i != null && i.nonEmpty
        }

        val result = spark.read.parquet("/home/mi/Develop/data/app-meta-info/total")
            .filter(emptyUdf($"introduction"))
            .as[EnhancedAppInfo]
            .map{ app =>
                val intro = MiTokenizer.segmentAndRemoveStopWords(app.introduction)
                    .map(_.name)
                    .filter(filter)
                    .mkString(" ")
                (app.displayName, app.packageName, intro)
            }
            .map(s => s._1 + "\t" + s._2 + "\t" + s._3)

//        val count = result.count()
//        bw.write(count + "\n")

        result
            .as[String]
            .collect
            .foreach(l => bw.write(l + "\n"))

        bw.flush()
        bw.close()
    }

    def filter(str: String) = {
        str.map{
            case t if t >= 0x4e00 && t <= 0x9fbb =>
                true
            case t if (t >= 'a' && t <= 'z') || (t >= 'A' && t <= 'Z') =>
                true
            case t if t >= '0' && t <= '9' =>
                true
            case _ =>
                false
        }.reduce(_ && _) && !isNumber(str)
    }

    def isNumber(str: String) = {
        str.map(c => (c >= '0' && c <= '9') || (c == '.')).reduce(_ && _)
    }
}
