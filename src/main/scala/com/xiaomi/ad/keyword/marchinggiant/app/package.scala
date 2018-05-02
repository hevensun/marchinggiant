package com.xiaomi.ad.keyword.marchinggiant.app

/**
  * Created by cailiming on 17-7-13.
  */
case class Category(catId: Int, catName: String, score: Double)
case class EmiCategory(name: String, score: Double)
case class LDATopic(topicId: Int, topicName: String, score: Double)

case class EnhancedAppInfo(
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
    category: Seq[Category],
    lda: Seq[LDATopic],
    source: Int
)

case class EnhancedAppInfoWithEmiCategory(
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
    category: Seq[Category],
    emiCategory: Seq[EmiCategory],
    lda: Seq[LDATopic],
    source: Int
)

case class OneSimilarity(packageName: String, displayName: String, score: Double)
case class SimilarityResult(packageName: String, displayName: String, apps: Seq[OneSimilarity])

case class AppDownload(appId: String, recordNum: Long, imeiNum: Int)
case class AppCoClick(query: String, appIdCountSeq: Seq[AppDownload])

case class AppPair(app1: Long, app2: Long, count: Long)

case class AppSimilarPair(app1: String, app2: String, label: String)

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

case class BaiduApp(
    packageName: String,
    displayName: String,
    crawlingName: String,
    level1CategoryName: String,
    level2CategoryName: String,
    introduction: Seq[String],
    brief: String,
    star: String,
    downloadCount: String
)

case class DetailCategory(packageName: String, displayName: String, category: String, score: Double)
case class DetailCategories(details: Seq[DetailCategory])

case class SimilarityResultWithCat(packageName: String, displayName: String, category: String, score: Double, apps: Seq[OneSimilarity])

case class CategoryWords(category: String, word: String, count: Long)

case class CategoryJoinWords(word: String, curCateCount: Long, otherCateCount: Option[Long])

case class TitleSummary(title: String, summary: String)

case class AppSnippets(
                          query: String,
                          baike: String,
                          relatedSearch: Seq[String],
                          ads: Seq[TitleSummary],
                          searchResult: Seq[TitleSummary]
                      )

case class AppSnippetsWithCategory(query: String, searchResult: String, googleCategories: Seq[Category])
case class AppId2Category(appId: Long, googleCategories: Seq[Category])