package com.xiaomi.ad.keyword.marchinggiant.appsimilarity

/**
  * Created by cailiming on 17-7-20.
  */
case class Category(catId: Int, catName: String, score: Double)
case class LDATopic(topicId: Int, topicName: String, score: Double)

case class OneSimilarity(packageName: String, displayName: String, score: Double)
case class SimilarityResult(packageName: String, displayName: String, apps: Seq[OneSimilarity])

case class AppDownload(appId: String, recordNum: Long, imeiNum: Int)
case class AppCoClick(query: String, appIdCountSeq: Seq[AppDownload])

case class AppPair(app1: Long, app2: Long, count: Long)

case class KeywordScore(keyword: String, score: Double)
case class CategoryScore(category: String, score: Double)

case class AppVectorWithName(packageName: String, displayName: String, introKeywords: Seq[KeywordScore], rawKeywords: Seq[KeywordScore], level1CategoryName: CategoryScore, level2CategoryName: CategoryScore, category: Seq[Category], topics: Seq[LDATopic])

case class AppVectorTwo(packageName1: String,
                        displayName1: String,
                        introKeywords1: Seq[KeywordScore],
                        rawKeywords1: Seq[KeywordScore],
                        level1CategoryName1: CategoryScore,
                        level2CategoryName1: CategoryScore,
                        category1: Seq[Category],
                        topics1: Seq[LDATopic],
                        packageName2: String,
                        displayName2: String,
                        introKeywords2: Seq[KeywordScore],
                        rawKeywords2: Seq[KeywordScore],
                        level1CategoryName2: CategoryScore,
                        level2CategoryName2: CategoryScore,
                        category2: Seq[Category],
                        topics2: Seq[LDATopic]
                       )

case class AppSimilarityWithName(app1: String, app1Name: String, app2: String, app2Name: String, score: Double) {
    @Override
    override def toString: String = {
        s"$app1\t$app1Name\t$app2\t$app2Name\t$score"
    }
}

case class EnhancedAppInfo2(
                               packageName: String,
                               appId: Long,
                               apkId: Long,
                               displayName: String,
                               level1CategoryName: String,
                               level2CategoryName: String,
                               publisherName: String,
                               introduction: String,
                               brief: String,
                               keywords: Seq[String],
                               categoryCategory: Seq[Category],
                               introductionCategory: Seq[Category],
                               keywordCategory: Seq[Category],
                               totalCategory: Seq[Category],
                               lda: Seq[LDATopic],
                               source: Int
                           )

case class EnhancedAppInfo3(
                               packageName: String,
                               appId: Long,
                               apkId: Long,
                               displayName: String,
                               level1CategoryName: String,
                               level2CategoryName: String,
                               publisherName: String,
                               introduction: String,
                               brief: String,
                               keywords: Seq[String],
                               introKeywords: Seq[ExtractKeyword],
                               categoryCategory: Seq[Category],
                               introductionCategory: Seq[Category],
                               keywordCategory: Seq[Category],
                               totalCategory: Seq[Category],
                               lda: Seq[LDATopic],
                               source: Int
                           )

case class ExtractKeyword(word: String, weight: Double)

case class AppVectorWithName2(
                                 packageName: String,
                                 displayName: String,
                                 introKeywords: Seq[KeywordScore],
                                 rawKeywords: Seq[KeywordScore],
                                 level1CategoryName: CategoryScore,
                                 level2CategoryName: CategoryScore,
                                 categoryCategory: Seq[Category],
                                 introductionCategory: Seq[Category],
                                 keywordCategory: Seq[Category],
                                 topics: Seq[LDATopic]
                             )

case class AppVectorTwo2(
                            packageName1: String,
                            displayName1: String,
                            introKeywords1: Seq[KeywordScore],
                            rawKeywords1: Seq[KeywordScore],
                            level1CategoryName1: CategoryScore,
                            level2CategoryName1: CategoryScore,
                            categoryCategory1: Seq[Category],
                            introductionCategory1: Seq[Category],
                            keywordCategory1: Seq[Category],
                            topics1: Seq[LDATopic],
                            packageName2: String,
                            displayName2: String,
                            introKeywords2: Seq[KeywordScore],
                            rawKeywords2: Seq[KeywordScore],
                            level1CategoryName2: CategoryScore,
                            level2CategoryName2: CategoryScore,
                            categoryCategory2: Seq[Category],
                            introductionCategory2: Seq[Category],
                            keywordCategory2: Seq[Category],
                            topics2: Seq[LDATopic]
                        )

object SIMILARITY_WEIGHT {
    val INTRO_KEYWORD = 1
    val RAW_KEYWORD = 0.8
    val LEVEL1_CATEGORY_NAME = 1
    val LEVEL2_CATEGORY_NAME = 2
    val CATEGORY_CATEGORY = 1
    val INTRO_CATEGORY = 0.7
    val KEYWORD_CATEGORY = 1
    val TOPIC = 1
}
