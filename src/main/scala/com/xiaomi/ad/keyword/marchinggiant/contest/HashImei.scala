package com.xiaomi.ad.keyword.marchinggiant.contest

import java.io.{BufferedWriter, OutputStreamWriter}

import com.twitter.scalding.Args
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by cailiming on 17-9-8.
  */
object HashImei {
    def main(args: Array[String]): Unit = {
        val argv = Args(args)
        execute(argv, new SparkConf())
    }

    def execute(args: Args, sparkConf: SparkConf) = {
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._

        val labelDate = "20170725,20170726,20170727,20170728,20170729,20170730,20170731,20170801,20170802,20170803,20170804,20170805,20170806,20170807,20170808,20170809,20170810,20170811,20170812,20170813,20170814,20170815,20170816"

        val ans = spark.read.text(s"/user/h_data_platform/platform/miuiads/contest_active_imei/date={$labelDate}")
            .as[String]
            .map{ l =>
                val splits = l.split("\t")
                splits.head
            }
            .distinct()
            .repartition(10000)
            .collect()
            .zipWithIndex

        writeMap("develop/cailiming/tmp/id-map/imei-mapping", ans)

        spark.stop()
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
}
