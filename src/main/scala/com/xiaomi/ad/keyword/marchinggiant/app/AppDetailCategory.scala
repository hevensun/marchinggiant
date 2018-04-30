package com.xiaomi.ad.keyword.marchinggiant.app

import com.twitter.scalding.Args
import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import scala.io.Source
import scala.util.Try
import scala.collection.JavaConversions._

/**
  * Created by cailiming on 17-12-4.
  */
object AppDetailCategory {
    def main(args: Array[String]): Unit = {
        val argv = Args(args)
        val sparkConf = new SparkConf()

        if(argv("pType") == "gen-cand") executeCandidate(argv, sparkConf)
        else if(argv("pType") == "gen-core") executeGenerateCore(argv, sparkConf)
        else if(argv("pType") == "gen-cate-words") executeGenCateWords(argv, sparkConf)
        else if(argv("pType") == "non-car") executeNonCar(argv, sparkConf)
        else executeCarClassify(argv, sparkConf)
    }

    def executeNonCar(args: Args, sparkConf: SparkConf) = {
        val spark = SparkSession
            .builder()
            .config(sparkConf)
            .getOrCreate()

        import spark.implicits._

        val categories = Seq("加油耗油", "汽车交易", "汽车养护", "汽车资讯", "行车辅助", "违章车险", "驾考摇号", "汽车金融", "充电服务", "共享租赁")

        val cateWords = categories
            .map { curCate =>
                val words = spark.read.text(args("cateWordsPath") + "/" + curCate)
                    .as[String]
                    .collect()
                    .map { line =>
                        val split = line.split("\t", 3)
                        split(1) -> split(2).toDouble
                    }
                    .filter(_._2 >= 1)
                    .toMap

                curCate -> words
            }

        val cateWordsBroadCast = spark.sparkContext.broadcast(cateWords)

        val appSet = Source.fromInputStream(AppDetailCategory.getClass.getResourceAsStream("/car-core"))
            .getLines()
            .filter(_.trim.nonEmpty)
            .map(l => l.split("\t").head)
            .toSet

        val appSetBroadCast = spark.sparkContext.broadcast(appSet)

        val pnUdf = udf { pn: String =>
            appSetBroadCast.value.contains(pn)
        }

        spark.read.parquet(args("input"))
            .select($"packageName", $"displayName", $"introduction", $"keywords")
            .filter(pnUdf($"packageName"))
            .map { row =>
                val packageName = row.getAs[String]("packageName")
                val displayName = row.getAs[String]("displayName")
                val introduction = row.getAs[String]("introduction")
                val keywords = row.getAs[Seq[String]]("keywords")


                val introWords = Try {
                    ToAnalysis.parse(introduction)
                        .getTerms
                        .map { t =>
                            t.getName
                        }
                }.getOrElse(Seq())

                val displayNameWords = Try {
                    ToAnalysis.parse(displayName)
                        .getTerms
                        .map { t =>
                            t.getName
                        }
                }.getOrElse(Seq())

                val keyWords = Try {
                    ToAnalysis.parse(keywords.mkString(" "))
                        .getTerms
                        .map { t =>
                            t.getName
                        }
                }.getOrElse(Seq())

                val allWords = (introWords ++ displayNameWords ++ keyWords).filter(_.trim.nonEmpty)

                val maxCnt = cateWordsBroadCast.value
                    .map { case(_, v) =>
                        allWords.count(a => v.contains(a))
                    }
                    .max

                val cntP = maxCnt * 1.0 / allWords.length

                f"$packageName\t$displayName\r$cntP%1.4f"
            }
            .repartition(1)
            .write
            .mode(SaveMode.Overwrite)
            .text(args("output"))

        spark.stop()
    }

    def executeCandidate(args: Args, sparkConf: SparkConf) = {
        val spark = SparkSession
            .builder()
            .config(sparkConf)
            .getOrCreate()

        import spark.implicits._

        val carCateBroadCast = spark.sparkContext.broadcast(
            Source.fromInputStream(AppDetailCategory.getClass.getResourceAsStream("/car_category.txt"))
                .getLines()
                .map { line =>
                    val split = line.split("\t")
                    split.head.toInt -> split(1)
                }
                .toMap
        )

        val carWords = spark.sparkContext.broadcast(
            Set("汽车", "车辆", "违章", "驾考", "摇号", "二手车", "车险", "行车", "加油站")
        )

        val nonCarWords = spark.sparkContext.broadcast(
            Set("游戏", "宝宝", "育儿", "公车", "巴士", "公交", "模拟器", "平衡车", "自行车", "订票", "酒店", "车票")
        )

        val togetherWords = spark.sparkContext.broadcast(
            Set("车", "加油")
        )

        spark.read.parquet(args("input"))
            .as[EnhancedAppInfoWithEmiCategory]
            .filter { a =>
                Try{
                    (a.level1CategoryName == null || a.level1CategoryName != "游戏") &&
                        a.introduction != null &&
//                        (carWords.value.exists(w => a.introduction.contains(w)) &&
//                        nonCarWords.value.map(w => !a.introduction.contains(w)).reduce(_ && _)) ||
                        togetherWords.value.map(w => a.introduction.contains(w)).reduce(_ && _)
                }.getOrElse(false)
            }
            .map { a =>
                val catStr = a.category.map(c => s"${c.catId}_${c.catName}_${c.score}").mkString(";")
                s"${a.packageName}\t${a.displayName}\t$catStr"
            }
            .repartition(1)
            .write
            .mode(SaveMode.Overwrite)
            .text(args("output"))

        spark.stop()
    }

    def executeGenerateCore(args: Args, sparkConf: SparkConf) = {
        val spark = SparkSession
            .builder()
            .config(sparkConf)
            .getOrCreate()

        import spark.implicits._

        val coreCarApp = spark.sparkContext.parallelize(
            Source.fromInputStream(AppDetailCategory.getClass.getResourceAsStream("/car-app1"))
                .getLines()
                .map { line =>
                    val split = line.split("\t")
                    DetailCategory(split.head, "", split.last, 1.0)
                }
                .toSeq
        ).toDF().as[DetailCategory]

        val candidatesBroadCast = spark.sparkContext.broadcast(
            Source.fromInputStream(AppDetailCategory.getClass.getResourceAsStream("/car-candidate"))
                .getLines()
                .map { line =>
                    val split = line.split("\t")
                    split.head
                }
                .toSet
        )

        val appWithSimilar = spark.read.parquet(args("input"))
            .select($"packageName", $"displayName", $"similarApps".alias("apps"))
            .as[SimilarityResult]

        val step1DF = oneStep(coreCarApp, appWithSimilar, spark)

        val ans = step1DF
            .filter { a =>
                candidatesBroadCast.value.contains(a.packageName) && a.score >= 0.92
            }
            .groupByKey(c => c.packageName)
            .mapGroups { case (_, cs) =>
                val csSeq = cs.toSeq
                if(csSeq.exists(_.score >= 0.95)) {
                    if(csSeq.exists(_.score == 1.0)) DetailCategories(csSeq.filter(_.score == 1.0))
                    else DetailCategories(csSeq.filter(_.score >= 0.95))
                } else {
                    DetailCategories(Seq(csSeq.maxBy(_.score)))
                }
            }
            .flatMap(_.details)

        ans
            .orderBy($"packageName", $"category", $"score".desc)
            .map { a =>
                f"${a.packageName}\t${a.displayName}\t${a.category}\t${a.score}%1.4f"
            }
            .write
            .mode(SaveMode.Overwrite)
            .text(args("output"))

        spark.stop()
    }

    def oneStep(coreDF: Dataset[DetailCategory], similarAppDF: Dataset[SimilarityResult], spark: SparkSession) = {
        import spark.implicits._

        coreDF
            .select($"packageName", $"category", $"score")
            .join(similarAppDF, Seq("packageName"), "LEFT")
            .as[SimilarityResultWithCat]
            .flatMap { sr =>
                val siApps = if(sr.apps != null) {
                    sr.apps
                        .map { a =>
                            DetailCategory(a.packageName, a.displayName, sr.category, sr.score * a.score)
                        }
                } else {
                    Seq()
                }

                siApps :+ DetailCategory(sr.packageName, sr.displayName, sr.category, sr.score)
            }
            .groupByKey(a => a.packageName + a.category)
            .mapGroups { case(_, apps) =>
                apps.toSeq
                    .maxBy(_.score)
            }
    }

    def executeGenCateWords(args: Args, sparkConf: SparkConf) = {
        val spark = SparkSession
            .builder()
            .config(sparkConf)
            .getOrCreate()

        import spark.implicits._

        val appCategory = Source.fromInputStream(AppDetailCategory.getClass.getResourceAsStream("/car-core"))
            .getLines()
            .filter(_.trim.nonEmpty)
            .map { line =>
                val split = line.split("\t")
                if(split.length == 4)
                    split.head -> split(2)
                else
                    split.head -> split(1)
            }
            .toMap

        val appCategoryBroadCast = spark.sparkContext.broadcast(appCategory)

        val pnUdf = udf { pn: String =>
            appCategoryBroadCast.value.contains(pn)
        }

        val allWords = spark.read.parquet(args("input"))
            .select($"packageName", $"displayName", $"introduction", $"keywords")
            .filter(pnUdf($"packageName"))
            .flatMap { row =>
                val packageName = row.getAs[String]("packageName")
                val displayName = row.getAs[String]("displayName")
                val introduction = row.getAs[String]("introduction")
                val keywords = row.getAs[Seq[String]]("keywords")


                val introWords = Try{
                    ToAnalysis.parse(introduction)
                        .getTerms
                        .map { t =>
                            CategoryWords(appCategoryBroadCast.value(packageName), t.getName, 1L)
                        }
                }.getOrElse(Seq())

                val displayNameWords = Try{
                    ToAnalysis.parse(displayName)
                        .getTerms
                        .map { t =>
                            CategoryWords(appCategoryBroadCast.value(packageName), t.getName, 10L)
                        }
                }.getOrElse(Seq())

                val keyWords = Try{
                    ToAnalysis.parse(keywords.mkString(" "))
                        .getTerms
                        .map { t =>
                            CategoryWords(appCategoryBroadCast.value(packageName), t.getName, 1L)
                        }
                }.getOrElse(Seq())

                (introWords ++ displayNameWords ++ keyWords).filter(_.word.trim.length > 1)
            }
            .groupBy($"category", $"word")
            .agg(count($"count").alias("count"))
            .as[CategoryWords]
            .filter(_.count > 1)

        allWords.cache()

        allWords.write.mode(SaveMode.Overwrite).format("parquet").save(args("output") + "/debug")

        appCategory
            .values
            .toSeq
            .distinct
            .foreach { curCate =>
                val curCateWords = allWords.filter(_.category == curCate)
                val otherCateWords = allWords.filter(_.category != curCate)
                    .groupBy($"word")
                    .agg(sum($"count").alias("otherCateCount"))

                curCateWords
                    .select($"word", $"count".alias("curCateCount"))
                    .join(otherCateWords, Seq("word"), "LEFT")
                    .as[CategoryJoinWords]
                    .map { cjw =>
                        val otherCateCount = if(cjw.otherCateCount.isDefined) cjw.otherCateCount.get * 1.0 else 1.0
                        f"$curCate\t${cjw.word}\t${cjw.curCateCount * 1.0 / otherCateCount}%1.4f"
                    }
                    .repartition(1)
                    .write
                    .mode(SaveMode.Overwrite)
                    .text(args("output") + "/" + curCate)
            }

        allWords.unpersist()
        spark.stop()
    }

    def executeCarClassify(args: Args, sparkConf: SparkConf) = {
        val spark = SparkSession
            .builder()
            .config(sparkConf)
            .getOrCreate()

        import spark.implicits._

        val categories = Seq("加油耗油", "汽车交易", "汽车养护", "汽车资讯", "行车辅助", "违章车险", "驾考摇号", "汽车金融", "充电服务", "共享租赁")

        val cateWords = categories
            .map { curCate =>
                val words = spark.read.text(args("cateWordsPath") + "/" + curCate)
                    .as[String]
                    .collect()
                    .map { line =>
                        val split = line.split("\t", 3)
                        split(1) -> split(2).toDouble
                    }
                    .filter(_._2 >= 1)
                    .toMap

                curCate -> words
            }

        val cateWordsBroadCast = spark.sparkContext.broadcast(cateWords)

        val candidatesBroadCast = spark.sparkContext.broadcast(
            Source.fromInputStream(AppDetailCategory.getClass.getResourceAsStream("/car-candidate"))
                .getLines()
                .map { line =>
                    val split = line.split("\t", 2)
                    split.head
                }
                .toSet
        )

        val coreAppBroadCast = spark.sparkContext.broadcast(
            Source.fromInputStream(AppDetailCategory.getClass.getResourceAsStream("/car-core"))
                .getLines()
                .filter(_.trim.nonEmpty)
                .map { line =>
                    val split = line.split("\t", 2)
                    split.head
                }
                .toSet
        )

        val pnUdf = udf { pn: String =>
            candidatesBroadCast.value.contains(pn) && !coreAppBroadCast.value.contains(pn)
        }

        val ans = spark.read.parquet(args("input"))
            .select($"packageName", $"displayName", $"introduction", $"keywords")
            .filter(pnUdf($"packageName"))
            .flatMap { row =>
                val packageName = row.getAs[String]("packageName")
                val displayName = row.getAs[String]("displayName")
                val introduction = row.getAs[String]("introduction")
                val keywords = row.getAs[Seq[String]]("keywords")

                val introWords = Try{
                    ToAnalysis.parse(introduction)
                        .getTerms
                        .map { t =>
                            t.getName
                        }
                }.getOrElse(Seq())

                val displayNameWords = Try{
                    ToAnalysis.parse(displayName)
                        .getTerms
                        .map { t =>
                            t.getName
                        }
                }.getOrElse(Seq())

                val keyWords = Try{
                    ToAnalysis.parse(keywords.mkString(" "))
                        .getTerms
                        .map { t =>
                            t.getName
                        }
                }.getOrElse(Seq())

                val allWords = (introWords ++ displayNameWords ++ keyWords).filter(_.trim.nonEmpty)

                val tmpScore = cateWordsBroadCast.value
                    .map { case(curCate, curCateWords) =>
                        val score = allWords.map(w => curCateWords.getOrElse(w, 0.0)).sum
                        DetailCategory(packageName, "", curCate, score)
                    }
                    .sortBy(-_.score)

                val refinedScore = if(tmpScore.head.category == "驾考摇号") {
                    if((tmpScore.head.score / tmpScore(1).score) >= 5) {
                        tmpScore
                    } else {
                        val newScore = tmpScore.head.score - tmpScore(1).score * 3
                        tmpScore.drop(1) :+ DetailCategory(tmpScore.head.packageName, "", "驾考摇号", if(newScore < 0) 0.0 else newScore)
                    }
                } else {
                    tmpScore
                }

                val sum = refinedScore.map(_.score).sum

                val refined = refinedScore
                    .map { c =>
                        val score = c.score / sum

                        (c.packageName, c.category, score)
                    }
                    .sortBy(-_._3)

                val first = refined.head
                val second = refined(1)

                val ans = if(first._2 == "驾考摇号") {
                    if(first._3 >= 0.8) {
                        Seq(first)
                    } else {
                        Seq(refined.filter(_._2 != "驾考摇号").maxBy(_._3))
                    }
                } else {
                    if(first._3 >= 0.6)
                        Seq(first)
                    else {
                        if(second._2 == "驾考摇号") {
                            Seq(first)
                        } else {
                            if(first._3 - second._3 <= 0.1) {
                                Seq(first, second)
                            } else {
                                Seq(first)
                            }
                        }
                    }
                }

                ans.map { case(p, c, s) =>
                    f"$p\t$c\t$s%1.4f"
                }
            }

        ans
            .repartition(1)
            .write
            .mode(SaveMode.Overwrite)
            .text(args("output"))

        spark.stop()
    }
}
