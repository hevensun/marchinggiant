package com.xiaomi.ad.keyword.marchinggiant.contest

import scala.io.Source

/**
  * Created by cailiming on 17-9-8.
  */
object LogLoss {
    def main(args: Array[String]): Unit = {
        execute1()
    }

    def execute() = {

        val predict = Source.fromFile("/home/mi/Desktop/tmp/sample_submission.csv")
            .getLines()
            .drop(1)
            .map{ l =>
                val splits = l.split(",")
                splits(1).toDouble
            }
            .toSeq

        val ans = Source.fromFile("/home/mi/Desktop/tmp/answer_30p.csv")
            .getLines()
            .drop(1)
            .map{ l =>
                val splits = l.split(",")
                splits(1).toDouble
            }
            .toSeq

        val pa = predict
            .zipWithIndex
            .map{ p =>
                p._1 -> ans(p._2)
            }

        println(pa.length)

        val logLoss = logloss(pa)
        println(logLoss)
    }

    def execute1() = {
        val pa = Source.fromFile("/home/mi/Desktop/fm_pre.txt")
            .getLines()
            .map{ l =>
                val split = l.split(" ")
                split.last.toDouble -> split.head.toDouble
            }
            .toSeq

        val logLoss = logloss(pa)
        println(logLoss)
    }

    def logloss(predictionAndLabels: Seq[(Double, Double)]) = {
        val loss = predictionAndLabels
            .map{ case (prediction, label) =>
                if(label > 0)
                    -Math.log(prediction)
                else
                    -Math.log(1 - prediction)
            }
            .sum
        loss / predictionAndLabels.length
    }
}
