package com.xiaomi.ad.keyword.marchinggiant.ner

/**
  * Created by cailiming on 17-10-27.
  */
case class BehaviorTag(
    imei1Md5: String,
    timeStamp: Long,
    sourceId: Int,
    actionId: Int,
    duration: Long,
    text: String,
    entityKey: String,
    extension: scala.collection.Map[String,String]
)
