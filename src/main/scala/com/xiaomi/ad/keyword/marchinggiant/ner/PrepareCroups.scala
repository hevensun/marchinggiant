package com.xiaomi.ad.keyword.marchinggiant.ner

import java.io.{BufferedWriter, File, FileWriter}

import org.ansj.splitWord.analysis.DicAnalysis

import scala.collection.JavaConversions._
import scala.io.Source

/**
  * Created by cailiming on 17-10-27.
  */
object PrepareCroups {
    def main(args: Array[String]): Unit = {
        exec("/home/mi/PycharmProjects/NER-LSTM-CRF/data/train.txt", "/home/mi/PycharmProjects/NER-LSTM-CRF/data/train_with_seg.txt")
    }

    def exec(input: String, output: String) = {
        val allLine = Source.fromFile(new File(input))
            .getLines()
            .toSeq

        val sentence = allLine.map { line =>
            if(line.isEmpty) "\n"
            else line.split(" ").head
        }.mkString("")

        println(allLine.length)
        println(sentence.length)

        val segStr = sentence
            .split("\n")
            .flatMap { curSen =>
                val ans = DicAnalysis.parse(curSen).getTerms.toList
                    .flatMap { term =>
                        if(term.getRealName.length == 1) Seq("0")
                        else {
                            term.getRealName.zipWithIndex
                                .map { case(_, i) =>
                                    if(i == 0) "1"
                                    else if(i == term.getRealName.length - 1) "3"
                                    else "2"
                                }
                        }
                    }

                ans :+ ""
            }

        println(segStr.length)

        val bw = new BufferedWriter(new FileWriter(new File(output)))

        allLine
            .zipWithIndex
            .foreach { case(l, i) =>
                val split = l.split(" ")
                if(l.isEmpty) bw.write("\n")
                else bw.write(split.head + " " + segStr(i) + " " + split.last + "\n")
            }

        bw.flush()
        bw.close()
    }
}
