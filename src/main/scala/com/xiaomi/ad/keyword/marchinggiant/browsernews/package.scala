package com.xiaomi.ad.keyword.marchinggiant.browsernews

/**
  * Created by cailiming on 17-8-25.
  */
case class OneWord(word: String, count: Int)
case class JoinTmp(word: String, curCount: Long, specCount: Long)
case class WordResult(word: String, curCount: Long, specCount: Long, score: Double)