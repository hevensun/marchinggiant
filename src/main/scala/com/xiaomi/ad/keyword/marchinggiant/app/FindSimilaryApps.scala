package com.xiaomi.ad.keyword.marchinggiant.app

import com.xiaomi.ad.matrix.qu.wordnormalizer.WordNormalizer
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import scopt.OptionParser

import scala.util.Try

/**
  * Created by cailiming on 17-7-11.
  */
object FindSimilaryApps {
    val RAW_KEYWORD_WEIGHT = 0.3

    case class Config(input: String, threshold: Double, vectorOutput: String, parquetOutput: String, textOutput: String)

    val configParser = new OptionParser[Config]("App Similarity") {
        override def showUsageOnError = true

        head("App Similarity", "1.0")

        opt[String]('i', "input") required() action {
            (i, cfg) => cfg.copy(input = i)
        } text "App Input Path."

        opt[Double]('t', "threshold") required() action {
            (t, cfg) => cfg.copy(threshold = t)
        } text "Day Path."

        opt[String]('v', "vectorOutput") required() action {
            (v, cfg) => cfg.copy(vectorOutput = v)
        } text "Vector Output Path."

        opt[String]('p', "parquetOutput") required() action {
            (p, cfg) => cfg.copy(parquetOutput = p)
        } text "Result Output Path."

        opt[String]('o', "textOutput") required() action {
            (o, cfg) => cfg.copy(textOutput = o)
        } text "Result Output Path."

        help("help") text "prints this usage text"
    }

    def main(args: Array[String]): Unit = {
        execute(args, new SparkConf())
    }

    def execute(args: Array[String], sparkConf: SparkConf): Unit = {
        for (config <- configParser.parse(args, Config(null, 0.0, null, null, null))) {
            val spark = SparkSession.builder().config(sparkConf).getOrCreate()
            import spark.implicits._

            val emptyUdf = udf { i: String =>
                i != null && i.nonEmpty
            }

            val vectorDf = spark.read.parquet(config.input)
                .filter(emptyUdf($"introduction"))
                .repartition(5)
                .as[EnhancedAppInfo]
                .map{ ea =>
                    val categoryMin = Try(ea.category.map(_.score).min).getOrElse(0.0)
                    val categoryMax = Try(ea.category.map(_.score).max).getOrElse(0.0)
                    val normalizedCategory = ea.category.map(c => c.copy(catName = null, score = minMaxNormalizer(categoryMin, categoryMax, c.score)))

                    val ldaMin = Try(ea.lda.map(_.score).min).getOrElse(0.0)
                    val ldaMax = Try(ea.lda.map(_.score).max).getOrElse(0.0)
                    val normalizedLda = ea.lda.map(l => l.copy(score = minMaxNormalizer(ldaMin, ldaMax, l.score)))

                    val lev1C = if(ea.level1CategoryName != null && ea.level1CategoryName.nonEmpty) 1.0 else 0.0
                    val lev2C = if(ea.level2CategoryName != null && ea.level2CategoryName.nonEmpty) 1.0 else 0.0

                    val rawK = if(ea.keywords != null) ea.keywords.length * Math.pow(RAW_KEYWORD_WEIGHT, 2) else 0.0

                    val de = lev1C + lev2C + rawK + normalizedCategory.map(c => Math.pow(c.score, 2)).sum + normalizedLda.map(t => Math.pow(t.score, 2)).sum
                    val sqrtDe = Math.sqrt(de)

                    val lev1Name = CategoryScore(ea.level1CategoryName, lev1C / sqrtDe)
                    val lev2Name = CategoryScore(ea.level2CategoryName, lev2C / sqrtDe)
                    val rawKeyword = Try(ea.keywords.map(k => KeywordScore(k, RAW_KEYWORD_WEIGHT / sqrtDe))).getOrElse(Seq())
                    val norCategory = normalizedCategory.map(c => c.copy(score = c.score / sqrtDe))
                    val norTopic = normalizedLda.map(l => l.copy(score = l.score / sqrtDe))

                    AppVectorWithName(ea.packageName, ea.displayName, Seq(), rawKeyword, lev1Name, lev2Name, norCategory, norTopic)
                }

            vectorDf
                .repartition(1)
                .write
                .mode(SaveMode.Overwrite)
                .format("parquet")
                .save(config.vectorOutput)

            val newColumns1 = vectorDf.columns.map(c => new StringContext(c).$().alias(c + "1")) :+ lit(1).alias("help")
            val newColumns2 = vectorDf.columns.map(c => new StringContext(c).$().alias(c + "2")) :+ lit(1).alias("help")
            val selectColumns = vectorDf.columns.flatMap(c => Seq(new StringContext(c + "1").$(), new StringContext(c + "2").$()))

            val vector1 = vectorDf.select(newColumns1: _*).repartition(100)
            val vector2 = vectorDf.select(newColumns2: _*).repartition(100)

            val resultDf = vector1
                .join(vector2, Seq("help"))
                .select(selectColumns: _*)
                .as[AppVectorTwo]
                .map{ at =>
                    val app1 = AppVectorWithName(at.packageName1, at.displayName1, at.introKeywords1, at.rawKeywords1, at.level1CategoryName1, at.level2CategoryName1, at.category1, at.topics1)
                    val app2 = AppVectorWithName(at.packageName2, at.displayName2, at.introKeywords2, at.rawKeywords2, at.level1CategoryName2, at.level2CategoryName2, at.category2, at.topics2)
                    if(app1.packageName == app2.packageName) {
                        AppSimilarityWithName("", "", "", "", 0.0)
                    } else {
                        val score = similarity(app1, app2)
                        AppSimilarityWithName(app1.packageName, WordNormalizer.normalize(app1.displayName), app2.packageName, WordNormalizer.normalize(app2.displayName), score)
                    }
                }
                .filter($"score" >= config.threshold)
                .orderBy($"app1", $"score".desc)
                .groupByKey(_.app1)
                .mapGroups{ case (app1, ts) =>
                    val tsseq = ts.toSeq

                    def scalaCustom(x: Double) = {
                        val roundBy = 4
                        val w = math.pow(10, roundBy)
                        (x * w).toLong.toDouble / w
                    }

                    val apps = tsseq.filter(t => scalaCustom(t.score) != 0.0)
                        .map(t => OneSimilarity(t.app2, t.app2Name, scalaCustom(t.score)))
                    SimilarityResult(app1, tsseq.head.app1Name, apps)
                }

            resultDf
                .repartition(50)
                .write
                .mode(SaveMode.Overwrite)
                .format("parquet")
                .save(config.parquetOutput)

            spark.read.parquet(config.parquetOutput)
                .as[SimilarityResult]
                .map{ s =>
                    s.displayName + "\t" + s.apps.map(a => a.displayName + "," + a.score).mkString(" ")
                }
                .write
                .mode(SaveMode.Overwrite)
                .text(config.textOutput)

            spark.stop()
        }
    }

    def minMaxNormalizer(min: Double, max: Double, x: Double) = {
        (x - min) / (max - min)
    }

    def similarity(v1: AppVectorWithName, v2: AppVectorWithName) = {
        val lev1Num = if(v1.level1CategoryName.category == null || v1.level1CategoryName.category.isEmpty || v2.level1CategoryName.category == null || v2.level1CategoryName.category.isEmpty || v1.level1CategoryName.category != v2.level1CategoryName.category) {
            0.0
        } else {
            v1.level1CategoryName.score * v2.level1CategoryName.score
        }

        val lev2Num = if(v1.level2CategoryName.category == null || v1.level2CategoryName.category.isEmpty || v2.level2CategoryName.category == null || v2.level2CategoryName.category.isEmpty || v1.level2CategoryName.category != v2.level2CategoryName.category) {
            0.0
        } else {
            v1.level2CategoryName.score * v2.level2CategoryName.score
        }

        val rawKeywordMap = v1.rawKeywords.map(k => k.keyword -> k.score).toMap
        val rawKeywordNum = v2.rawKeywords
            .map{ k =>
                k.score * Try(rawKeywordMap(k.keyword)).getOrElse(0.0)
            }
            .sum

        val categoryMap = v1.category.map(c => c.catId -> c.score).toMap
        val cateNum = v2.category
            .map{ c =>
                c.score * Try(categoryMap(c.catId)).getOrElse(0.0)
            }
            .sum

        val topicMap = v1.topics.map(t => t.topicId -> t.score).toMap
        val topicNum = v2.topics
            .map{ t =>
                t.score * Try(topicMap(t.topicId)).getOrElse(0.0)
            }
            .sum

        val numerator = lev1Num + lev2Num + rawKeywordNum + cateNum + topicNum
        numerator
    }
}
