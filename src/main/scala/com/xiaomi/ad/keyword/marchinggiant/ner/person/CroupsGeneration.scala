package com.xiaomi.ad.keyword.marchinggiant.ner.person

import java.io.{BufferedWriter, File, FileWriter}

import scala.io.Source

/**
  * Created by cailiming on 17-11-6.
  */
object CroupsGeneration {
    def main(args: Array[String]): Unit = {
        val firstNameDic = Source.fromInputStream(getClass.getResourceAsStream("/fnms.txt"))
            .getLines()
            .filter { line =>
                line.split("\t")(1).toInt >= 0
            }
            .map(l => l.split("\t").head)
            .toSet

        val bw = new BufferedWriter(new FileWriter(new File("/home/mi/Documents/Resources/croups/person_hmm.txt")))

        Source.fromFile("/home/mi/Documents/Resources/croups/seg-croups/corpusZh/person_part.txt")
            .getLines()
            .filter { line =>
                val str = line.split("\t\t ").last
                val names = str.split(" ")
                    .filter(_.contains("nh"))

                names.map { name =>
                    (firstNameDic.contains(name.substring(0, 1)) || (name.split("/").head.length >= 2 && firstNameDic.contains(name.substring(0, 2)))) && !name.contains("·")
                }
                .reduce(_ && _)
            }
            .map { line =>
                val str = line.split("\t\t ").last

                val words = str.split(" ")
                    .map { cr =>
                        val curSplit = cr.split("/")
                        curSplit.head -> curSplit.last
                    }
                    .zipWithIndex

                val nameWords = words.filter { case(info, index) =>
                    info._2.contains("nh")
                }

                val tagBuilder = new StringBuilder
                words.foreach { case(_, _) =>
                    tagBuilder.append("A")
                }

                nameWords.foreach { case(info, index) =>
                    if(info._2 == "nhf") {
                        if(index - 1 >= 0 && words(index - 1)._2 == "h") {
                            tagBuilder(index - 1) = 'F'
                            tagBuilder(index) = 'B'
                            if(index - 2 >= 0) {
                                tagBuilder(index - 2) = 'K'
                            }
//                            if(index + 2 < words.length && words(index + 2)._2)
                        }
                    }
                }
            }
            .foreach(line => bw.write(line + "\n"))

        bw.flush()
        bw.close()
    }

    def isBBCF(words: Seq[(String, String)], i: Int) = {

    }

    def isFB(words: Seq[(String, String)], i: Int) = {

    }


}
