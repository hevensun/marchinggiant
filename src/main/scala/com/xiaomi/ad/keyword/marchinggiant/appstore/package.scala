package com.xiaomi.ad.keyword.marchinggiant.appstore

/**
  * Created by cailiming on 17-7-27.
  */
case class AppQueryResponse(query: String, appIds: String)
case class QueryAppPVUV(query: String, appId: String, pv: Long, uv: Long)

case class AppDownload(appId: String, recordNum: Long, imeiNum: Int)
case class AppCoClick(query: String, appIdCountSeq: Seq[AppDownload])

case class ItemInfo(title: String, summary: String)
case class QueryInfo(
    query: String,
//    packageName: String = "",
    baike: String = "",
    relatedSearch: Seq[String] = Seq.empty[String],
    ads: Seq[ItemInfo] = Seq.empty[ItemInfo],
    searchResult: Seq[ItemInfo] = Seq.empty[ItemInfo]
)

case class RelateQueryInfo(
    query: String,
    baike: String = "",
    relatedSearch: Seq[String] = Seq.empty[String],
    ads: Seq[ItemInfo] = Seq.empty[ItemInfo],
    searchResult: Seq[ItemInfo] = Seq.empty[ItemInfo]
)

case class AppInfo(
                  app_id: Long,
                  app_description: String,
                  app_category1: String,
                  app_category2: String
                  )