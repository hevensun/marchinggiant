package com.xiaomi.ad.keyword.marchinggiant.adinfo

import java.io.{BufferedWriter, OutputStreamWriter}

import com.twitter.scalding.Args
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by cailiming on 17-8-30.
  */
object AdInfoJob {
    def main(args: Array[String]): Unit = {
        val argv = Args(args)
        execute(argv, new SparkConf())
    }

    def execute(args: Args, sparkConf: SparkConf) = {
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._

        val adInfo = spark.read.parquet("matrix/relevance/ad_info")
            .as[AdItem]
            .filter(_.adType == 1)
            .map{ ad =>
                val tagsStr = ad.tags.getOrElse(Seq()).map(_.replace(",", "-")).mkString(":")
                val assetInfo = ad.assetInfo.getOrElse(AssetItem(Some(Seq()), Some(Seq()), Some(-1), Some(Seq()), Some(0L)))

                val assetTitle = assetInfo.titles.getOrElse(Seq()).map(_.replace(",", "-")).mkString(":")
                val assetImg = assetInfo.imgUrls.getOrElse(Seq()).map(_.replace(",", "-")).mkString(":")

                s"${ad.adId},${getOptionStr(ad.adName)},${getOptionStr(ad.landingPageUrl)},${getOptionStr(ad.companyName)}" +
                    s",${getOptionStr(ad.subCompanyName)},${getOptionStr(ad.level1Industry)},${getOptionStr(ad.level2Industry)}" +
                    s",${getOptionStr(ad.website)},$tagsStr,$assetTitle,$assetImg"
            }

        val titleStr = "adId,adName,landingPageUrl,companyName,subCompanyName,level1Industry,level2Industry,website,tags,assetTitle,assetImg\n"

        val resultBuilder = new StringBuilder
        resultBuilder.append(titleStr)
        adInfo
            .collect()
            .foreach(l => resultBuilder.append(l + "\n"))

        val fs = FileSystem.get(new Configuration())
        val file = new Path("develop/cailiming/ad-info-text")
        val bw = new BufferedWriter(new OutputStreamWriter(fs.create(file), "UTF-8"))
        bw.write(resultBuilder.toString())
        bw.close()
        fs.close()
    }

    def getOptionStr(str: Option[String]) = {
        str.getOrElse("").replace(",", "-")
    }
}
