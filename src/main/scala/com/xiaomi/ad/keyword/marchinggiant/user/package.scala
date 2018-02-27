package com.xiaomi.ad.keyword.marchinggiant.user

/**
  * Created by cailiming on 18-1-18.
  */
case class UserCategories(imeiMd5: String, lev1Categories: Seq[(String, Int)], lev2Categories: Seq[(String, Int)])

case class UserEmiCategories(imeiMd5: String, categories: Seq[(String, Int)])

case class UserGoogleCategories(imeiMd5: String, categories: Map[Long, Seq[String]])

case class AdEvent(imei1Md5: String, timeStamp: Long)

case class UserBehaviorsCate(imeiMd5: String, behaviors: Seq[(String, Int)])

case class AppTags(packageName: String, displayName: String, tags: Seq[String])

case class UserAppTags(imeiMd5: String, appTags: Map[String, Int])

case class AppDownloadBehaviors(pair1: Seq[String], pair2: Seq[String], pair3: Seq[String], pair4: Seq[String], pair5: Seq[String], pair6: Seq[String])
