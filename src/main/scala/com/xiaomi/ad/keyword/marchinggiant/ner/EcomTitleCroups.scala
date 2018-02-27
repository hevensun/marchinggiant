package com.xiaomi.ad.keyword.marchinggiant.ner

import java.io.{BufferedWriter, FileWriter}

import org.ansj.library.DicLibrary
import org.ansj.splitWord.analysis.ToAnalysis

import scala.io.Source
import scala.collection.JavaConversions._

/**
  * Created by cailiming on 17-12-21.
  */
object EcomTitleCroups {
    val brandDic = Source.fromFile("/home/mi/Documents/Resources/croups/电商/title-ner/brand_clothing_outdoors.txt")
        .getLines()
        .toSet

    val colorDic = Source.fromFile("/home/mi/Documents/Resources/croups/电商/title-ner/color_clothing_outdoors.txt")
        .getLines()
        .toSet

    val materialDic = Source.fromFile("/home/mi/Documents/Resources/croups/电商/title-ner/material_clothing_outdoors.txt")
        .getLines()
        .toSet

    val objectDic = Source.fromFile("/home/mi/Documents/Resources/croups/电商/title-ner/object_clothing_outdoors.txt")
        .getLines()
        .toSet

    val productDic = Source.fromFile("/home/mi/Documents/Resources/croups/电商/title-ner/product_clothing_outdoors.txt")
        .getLines()
        .toSet

    val seasonDic = Source.fromFile("/home/mi/Documents/Resources/croups/电商/title-ner/season_clothing_outdoors.txt")
        .getLines()
        .toSet

    val thicknessDic = Source.fromFile("/home/mi/Documents/Resources/croups/电商/title-ner/thickness_clothing_outdoors.txt")
        .getLines()
        .toSet

    val dicMap = Map(
        "BRAND" -> brandDic,
        "COLOR" -> colorDic,
        "MATERIAL" -> materialDic,
        "OBJECT" -> objectDic,
        "PRODUCT" -> productDic,
        "SEASON" -> seasonDic,
        "THICKNESS" -> thicknessDic
    )

    def main1(args: Array[String]): Unit = {
        val bw = new BufferedWriter(new FileWriter("/home/mi/Desktop/croups_clothing_outdoors_vip.txt"))

        val rawCroups = Source.fromFile("/home/mi/Documents/Resources/croups/电商/vipProductName.txt")
            .getLines()
            .toSeq
            .map { line =>
                line.replace("\t", " ").toLowerCase.replace(" ", "#").trim
            }
            .take(25000)

        rawCroups.map { line =>
            println(line)
            val canAtts = dicMap
                .flatMap { case(attName, attDic) =>
                    attDic
                        .flatMap { word =>
                            word.r.findAllMatchIn(line)
                                .map { m =>
                                    m.start -> word
                                }
                        }
                        .groupBy(_._1)
                        .map { case(start, vs) =>
                            vs.toSeq.maxBy(_._2.length)
                        }
                        .map { case(start, v) =>
                            (start, v, attName)
                        }
                }
                .groupBy(_._1)
                .map { case(start, vs) =>
                    vs.toSeq.maxBy(_._2.length)
                }
                .toSeq
                .sortBy(_._1)

            val sePair = canAtts.map(a => a._1 -> (a._1 + a._2.length - 1))

            val ansAtts = canAtts.filter { case(start, v, att) =>
                val cd = sePair.exists { case(s, e) =>
                    start > s && start <= e
                }

                val zm = (isLetter(v.charAt(0)) && (start - 1 >= 0 && isLetter(line.charAt(start - 1)))) ||
                    (isLetter(v.last) && (start + v.length < line.length && isLetter(line.charAt(start + v.length))))

                val mao = v == "帽" && start - 1 >= 0 && (line.charAt(start - 1) == '连' || line.charAt(start - 1) == '带')

                val hong = v == "红" && start + 1 < line.length && (line.charAt(start + 1) == '人')

                !cd && !zm && !mao && !hong
            }

            line -> ansAtts
        }
        .filter(_._2.exists(_._3 == "PRODUCT"))
        .foreach { case(line, canAtts) =>
            val ans = line + "\t" + canAtts.map(a => a._1 + "/" + a._2 + "/" + a._3).mkString(";")

            bw.write(ans + "\n")
        }

        bw.flush()
        bw.close()

    }

    def isLetter(a: Char) = {
        (a >= 'a' && a <= 'z') || a == '-' || (a >= '0' && a <= '9')
    }

    def main3(args: Array[String]): Unit = {
        val bw = new BufferedWriter(new FileWriter("/home/mi/Desktop/fu_clothing_outdoors.txt"))

        val can = Source.fromFile("/home/mi/Documents/Resources/croups/电商/vipProductName.txt")
            .getLines()
            .toSeq
            .flatMap { line =>
                "服".r.findAllMatchIn(line)
                    .flatMap { m =>
                        val start = m.start
                        val uni = if(start - 1 >= 0) {
                            "" + line.charAt(start - 1) + line.charAt(start)
                        } else ""

                        val bi = if(start - 2 >= 0) {
                            "" + line.charAt(start - 2) + line.charAt(start - 1) + line.charAt(start)
                        } else ""

                        Seq(uni, bi)
                    }
                    .toSeq
                    .map(a => a -> 1)
            }
            .filter(_._1.trim.nonEmpty)
            .groupBy(_._1)
            .mapValues(_.length)
            .toSeq
            .filter(_._2 >= 10)
            .sortBy(-_._2)

        val removed = scala.collection.mutable.MutableList.newBuilder[String]

        can.foreach { case(word, cnt) =>
            val endsWith =  can.filter(c => c._1.endsWith(word) && c._1 != word)

            if(endsWith.nonEmpty) {
                val maxCnt = endsWith.maxBy(_._2)._2

                if(cnt - maxCnt > 100) {
                    removed ++= can.filter(_._1.endsWith(word)).map(_._1)
                } else {
                    removed += word
                }
            }
        }

        val removedSet = removed.result().toSet

        val ans = can.filter(c => !removedSet.contains(c._1))

        ans.foreach(a => bw.write(a._1 + "\t" + a._2 + "\n"))

        bw.flush()
        bw.close()
    }

    def main2(args: Array[String]): Unit = {
        val bw = new BufferedWriter(new FileWriter("/home/mi/Desktop/color_clothing_outdoors.txt"))

        Source.fromFile("/home/mi/Documents/Resources/croups/电商/title-ner/color_clothing_outdoors.txt")
            .getLines()
            .filter { line =>
                !line.map(c => c >= '0' && c <= '9').reduce(_ || _)
            }
            .foreach { l =>
                bw.write(l + "\n")
            }

        bw.flush()
        bw.close()
    }

    def main5(args: Array[String]): Unit = {
        val NUM_STR = "N#UM"

        val bw = new BufferedWriter(new FileWriter("/home/mi/PycharmProjects/DL-NER/source/bioes_train_clothing_outdoors.txt"))

        Source.fromFile("/home/mi/Documents/Resources/croups/电商/title-ner/croups/train_clothing_outdoors.txt")
            .getLines()
            .flatMap { line =>
                val split = line.split("\t")
                val entities = split.last.split(";")
                    .map { e =>
                        val curS = e.split("/")
                        curS.head.toInt -> (curS(1), curS.last)
                    }
                    .toMap

                val product = split.head.trim.map { c =>
                    if((c >= 0x00 && c < 0x20) || c == 0x7F) '#' else c
                }.mkString("")

                val terms = ToAnalysis.parse(product)
                    .getTerms
                    .flatMap { term =>
                        if(term.getNatureStr == "en")
                            Seq(term.getName -> "S")
                        else if(term.getNatureStr == "m")
                            Seq(term.getName + NUM_STR -> "S")
                        else {
                            if(term.getName.length == 1) {
                                Seq(term.getName -> "S")
                            } else {
                                term.getName
                                    .zipWithIndex
                                    .map { case(w, i) =>
                                        if(i == 0) {
                                            (w + "") -> "B"
                                        } else if(i == term.getName.length - 1) {
                                            (w + "") -> "E"
                                        } else {
                                            (w + "") -> "I"
                                        }
                                    }
                            }
                        }
                    }

                var index = 0
                var isStarted = false
                var lastStart = 0
                val ans = terms
                    .map { wordIndex =>
                        val word = wordIndex._1
                        val segIndex = wordIndex._2
                        val str = if(entities.contains(index)) {
                            if(word.length == entities(index)._1.length) {
                                word + "\t" + segIndex + "\tS-" + entities(index)._2 + "\n"
                            } else {
                                isStarted = true
                                lastStart = index
                                word + "\t" + segIndex + "\tB-" + entities(index)._2 + "\n"
                            }
                        } else if(isStarted) {
                            if((index + word.length) >= (lastStart + entities(lastStart)._1.length)) {
                                isStarted = false
                                word + "\t" + segIndex + "\tE-" + entities(lastStart)._2 + "\n"
                            } else {
                                word + "\t" + segIndex + "\tI-" + entities(lastStart)._2 + "\n"
                            }
                        } else {
                            word + "\t" + segIndex + "\tO\n"
                        }

                        val length = if(word.endsWith(NUM_STR)) word.replace(NUM_STR, "").length else word.length
                        index = index + length

                        str
                    }

                ans :+ "\n"
            }
            .map { line =>
                val split = line.split("\t")
                if(split.head.endsWith(NUM_STR)) {
                    processNum(split.head.replace(NUM_STR, "")) + "\t" + split.drop(1).mkString("\t")
                } else {
                    line
                }
            }
            .foreach { l =>
                bw.write(l)
            }

        bw.flush()
        bw.close()
    }

    def main(args: Array[String]): Unit = {
        val NUM_STR = "N#UM"

        val bw = new BufferedWriter(new FileWriter("/home/mi/PycharmProjects/DL-NER/source/bioes_4predict_clothing_outdoors.txt"))

        val input1 = Source.fromFile("/home/mi/Documents/Resources/croups/电商/tbProductName.txt")
            .getLines()
            .toSeq

        val input2 = Source.fromFile("/home/mi/Documents/Resources/croups/电商/vipProductName.txt")
            .getLines()
            .toSeq

        val input = input1.union(input2)

        input
            .flatMap { line =>
                val product = line.trim.map { c =>
                    if((c >= 0x00 && c < 0x20) || c == 0x7F) '#' else c
                }.mkString("")

                val ans = ToAnalysis.parse(product)
                    .getTerms
                    .flatMap { term =>
                        if(term.getNatureStr == "en")
                            Seq(term.getName + "\tS\n")
                        else if(term.getNatureStr == "m")
                            Seq(processNum(term.getName) + "\tS\n")
                        else {
                            if(term.getName.length == 1) {
                                Seq(term.getName + "\tS\n")
                            } else {
                                term.getName
                                    .zipWithIndex
                                    .map { case(w, i) =>
                                        if(i == 0) {
                                            (w + "") + "\tB\n"
                                        } else if(i == term.getName.length - 1) {
                                            (w + "") + "\tE\n"
                                        } else {
                                            (w + "") + "\tI\n"
                                        }
                                    }
                            }
                        }
                    }

                ans :+ "\n"
            }
            .foreach { l =>
                bw.write(l)
            }

        bw.flush()
        bw.close()
    }

    def processNum(input: String) = {
        val ans = new StringBuilder
        ans.append("0")
        input.foreach { c =>
            if((c >= '0' && c <= '9') || c == '.') {}
            else ans.append(c)
        }
        ans.toString
    }
}
