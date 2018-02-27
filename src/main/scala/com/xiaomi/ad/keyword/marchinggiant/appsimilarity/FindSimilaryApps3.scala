package com.xiaomi.ad.keyword.marchinggiant.appsimilarity

import com.xiaomi.ad.matrix.qu.wordnormalizer.WordNormalizer
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import scopt.OptionParser

import scala.util.Try

/**
  * Created by cailiming on 17-7-11.
  */
object FindSimilaryApps3 {

    case class Config(input: String, introKeyword: String, threshold: Double, vectorOutput: String, parquetOutput: String, textOutput: String)

    val configParser = new OptionParser[Config]("App Similarity") {
        override def showUsageOnError = true

        head("App Similarity", "1.0")

        opt[String]('i', "input") required() action {
            (i, cfg) => cfg.copy(input = i)
        } text "App Input Path."

        opt[String]('k', "introKeyword") required() action {
            (i, cfg) => cfg.copy(introKeyword = i)
        } text "Intro keywords Input Path."

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
        for (config <- configParser.parse(args, Config(null, null, 0.0, null, null, null))) {
            val spark = SparkSession.builder().config(sparkConf).getOrCreate()
            import spark.implicits._

            val emptyUdf = udf { i: String =>
                i != null && i.nonEmpty
            }

            val introKeywordDf = spark.read.parquet(config.introKeyword)
                .select($"pn".alias("packageName"), $"kw".alias("introKeywords"))

            val vectorDf = spark.read.parquet(config.input)
                .filter(emptyUdf($"introduction"))
                .join(introKeywordDf, Seq("packageName"))
                .repartition(5)
                .as[EnhancedAppInfo3]
                .map{ ea =>
                    val normalizedCategoryCategory = normalizeCategory(ea.categoryCategory, SIMILARITY_WEIGHT.CATEGORY_CATEGORY)

                    val normalizedKeywordCategory = normalizeCategory(ea.keywordCategory, SIMILARITY_WEIGHT.KEYWORD_CATEGORY)

                    val normalizedIntroCategory = normalizeCategory(ea.introductionCategory, SIMILARITY_WEIGHT.INTRO_CATEGORY)

                    val normalizedLda = normalizeLDA(ea.lda, SIMILARITY_WEIGHT.TOPIC)

                    val normalizedIntroKeyword = normalizeIntroKeyword(ea.introKeywords, SIMILARITY_WEIGHT.INTRO_KEYWORD)


                    val lev1C = if(ea.level1CategoryName != null && ea.level1CategoryName.nonEmpty) SIMILARITY_WEIGHT.LEVEL1_CATEGORY_NAME else 0.0
                    val lev2C = if(ea.level2CategoryName != null && ea.level2CategoryName.nonEmpty) SIMILARITY_WEIGHT.LEVEL2_CATEGORY_NAME else 0.0

                    val rawKeyword = if(ea.keywords != null) ea.keywords.length * Math.pow(SIMILARITY_WEIGHT.RAW_KEYWORD, 2) else 0.0

                    val de = lev1C + lev2C + rawKeyword +
                        normalizedCategoryCategory.map(c => Math.pow(c.score, 2)).sum +
                        normalizedKeywordCategory.map(c => Math.pow(c.score, 2)).sum +
                        normalizedIntroCategory.map(c => Math.pow(c.score, 2)).sum +
                        normalizedLda.map(t => Math.pow(t.score, 2)).sum +
                        normalizedIntroKeyword.map(k => Math.pow(k.score, 2)).sum

                    val sqrtDe = Math.sqrt(de)

                    val norLev1CName = CategoryScore(ea.level1CategoryName, lev1C / sqrtDe)
                    val norLev2CName = CategoryScore(ea.level2CategoryName, lev2C / sqrtDe)
                    val norRawKeyword = Try(ea.keywords.map(k => KeywordScore(k, SIMILARITY_WEIGHT.RAW_KEYWORD / sqrtDe))).getOrElse(Seq())
                    val norCategoryCategory = normalizedCategoryCategory.map(c => c.copy(score = c.score / sqrtDe))
                    val norKeywordCategory = normalizedKeywordCategory.map(c => c.copy(score = c.score / sqrtDe))
                    val norIntroCateogry = normalizedIntroCategory.map(c => c.copy(score = c.score / sqrtDe))
                    val norTopic = normalizedLda.map(l => l.copy(score = l.score / sqrtDe))
                    val norIntroKeyword = normalizedIntroKeyword.map(k => k.copy(score = k.score / sqrtDe))

                    AppVectorWithName2(ea.packageName, ea.displayName, norIntroKeyword, norRawKeyword, norLev1CName, norLev2CName, norCategoryCategory, norIntroCateogry, norKeywordCategory, norTopic)
                }

            vectorDf
                .repartition(1)
                .write
                .mode(SaveMode.Overwrite)
                .format("parquet")
                .save(config.vectorOutput)

            //做笛卡尔集
            val newColumns1 = vectorDf.columns.map(c => new StringContext(c).$().alias(c + "1")) :+ lit(1).alias("help")
            val newColumns2 = vectorDf.columns.map(c => new StringContext(c).$().alias(c + "2")) :+ lit(1).alias("help")
            val selectColumns = vectorDf.columns.flatMap(c => Seq(new StringContext(c + "1").$(), new StringContext(c + "2").$()))

            val vector1 = vectorDf.select(newColumns1: _*).repartition(100)
            val vector2 = vectorDf.select(newColumns2: _*).repartition(100)

            val resultDf = vector1
                .join(vector2, Seq("help"))
                .select(selectColumns: _*)
                .as[AppVectorTwo2]
                .map{ at =>
                    val app1 = AppVectorWithName2(at.packageName1, at.displayName1, at.introKeywords1, at.rawKeywords1, at.level1CategoryName1, at.level2CategoryName1, at.categoryCategory1, at.introductionCategory1, at.keywordCategory1, at.topics1)
                    val app2 = AppVectorWithName2(at.packageName2, at.displayName2, at.introKeywords2, at.rawKeywords2, at.level1CategoryName2, at.level2CategoryName2, at.categoryCategory2, at.introductionCategory2, at.keywordCategory2, at.topics2)
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
                        val roundBy = 6
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
                .repartition(50)
                .write
                .mode(SaveMode.Overwrite)
                .text(config.textOutput)

            spark.stop()
        }
    }

    /**
      * 采用最大最小归一化
      * @param cates
      * @return
      */
    def normalizeCategory(cates: Seq[Category], weight: Double) = {
        val min = Try(cates.map(_.score).min).getOrElse(0.0)
        val max = Try(cates.map(_.score).max).getOrElse(0.0)
        cates.map(c => c.copy(catName = null, score = weight * minMaxNormalizer(min, max, c.score)))
    }

    /**
      * 采用最大最小归一化
      * @param ldas
      * @return
      */
    def normalizeLDA(ldas: Seq[LDATopic], weight: Double) = {
        val min = Try(ldas.map(_.score).min).getOrElse(0.0)
        val max = Try(ldas.map(_.score).max).getOrElse(0.0)
        ldas.map(l => l.copy(score = weight * minMaxNormalizer(min, max, l.score)))
    }

    def normalizeIntroKeyword(keywords: Seq[ExtractKeyword], weight: Double) = {
        val min = Try(keywords.map(_.weight).min).getOrElse(0.0)
        val max = Try(keywords.map(_.weight).max).getOrElse(0.0)
        keywords.map(k => KeywordScore(k.word, weight * minMaxNormalizer(min, max, k.weight)))
    }

    def minMaxNormalizer(min: Double, max: Double, x: Double) = {
        (x - min) / (max - min)
    }

    def similarity(v1: AppVectorWithName2, v2: AppVectorWithName2) = {
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

        val categoryCategoryMap = v1.categoryCategory.map(c => c.catId -> c.score).toMap
        val cateCateNum = v2.categoryCategory
            .map{ c =>
                c.score * Try(categoryCategoryMap(c.catId)).getOrElse(0.0)
            }
            .sum

        val introCategoryMap = v1.introductionCategory.map(c => c.catId -> c.score).toMap
        val introCateNum = v2.introductionCategory
            .map{ c =>
                c.score * Try(introCategoryMap(c.catId)).getOrElse(0.0)
            }
            .sum

        val keyworCategoryMap = v1.keywordCategory.map(c => c.catId -> c.score).toMap
        val keywordCateNum = v2.keywordCategory
            .map{ c =>
                c.score * Try(keyworCategoryMap(c.catId)).getOrElse(0.0)
            }
            .sum

        val topicMap = v1.topics.map(t => t.topicId -> t.score).toMap
        val topicNum = v2.topics
            .map{ t =>
                t.score * Try(topicMap(t.topicId)).getOrElse(0.0)
            }
            .sum

        val introKeywordMap = v1.introKeywords.map(k => k.keyword -> k.score).toMap
        val introKeywordNum = v2.introKeywords
            .map{ k =>
                k.score * Try(introKeywordMap(k.keyword)).getOrElse(0.0)
            }
            .sum

        lev1Num + lev2Num + rawKeywordNum + cateCateNum + introCateNum + keywordCateNum + topicNum + introKeywordNum
    }
}
