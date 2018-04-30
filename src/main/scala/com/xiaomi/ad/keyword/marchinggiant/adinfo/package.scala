package com.xiaomi.ad.keyword.marchinggiant.adinfo

/**
  * Created by cailiming on 17-8-30.
  */
case class CatItem(
                      id: Int,
                      catName: String,
                      score: Double
                  )
case class AppItem(
                      packageName: Option[String],
                      appId: Long,
                      level1Category: Option[String],
                      level2Category: Option[String],
                      displayName: Option[String],
                      keywords: Option[Seq[String]],
                      levelClassifies: Option[Seq[CatItem]],
                      keywordsClassifies: Option[Seq[CatItem]]
                  )
case class AssetItem(
                        titles: Option[Seq[String]],
                        imgUrls: Option[Seq[String]],
                        level: Option[Int],
                        titlesClassifies: Option[Seq[CatItem]],
                        assetId: Option[Long]
                    )

case class AdItem(
                     adId: Long,
                     adName: Option[String],
                     adType: Int,
                     billingType: Int,
                     placementType: Option[String],
                     templateId: Option[String],
                     campaignId: Option[String],
                     groupId: Option[Long],
                     bidPrice: Option[Double],
                     landingPageUrl: Option[String],
                     beginTime: Option[String],
                     endTime: Option[String],
                     sexTarget: Option[Seq[String]],
                     ageTarget: Option[Seq[String]],
                     provinceTarget: Option[Seq[String]],
                     timeTarget: Option[Seq[String]],
                     companyName: Option[String],
                     subCompanyName: Option[String],
                     level1Industry: Option[String],
                     level2Industry: Option[String],
                     website: Option[String],
                     levelClassifies: Option[Seq[CatItem]],
                     tags: Option[Seq[String]],
                     tagsClassifies: Option[Seq[CatItem]],
                     assetInfo: Option[AssetItem],
                     appInfo: Option[AppItem],
                     xiamiId: Option[Long]
                 )
