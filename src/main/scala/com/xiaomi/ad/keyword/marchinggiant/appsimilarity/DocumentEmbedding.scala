package com.xiaomi.ad.keyword.marchinggiant.appsimilarity

import java.io.{BufferedWriter, FileWriter}

import breeze.linalg.DenseVector
import scopt.OptionParser

import scala.io.Source

/**
  * Created by cailiming on 17-7-19.
  */
object DocumentEmbedding {
    case class Config(theta: String, topicVector: String, appName: String, output: String)

    val configParser = new OptionParser[Config]("App Similarity") {
        override def showUsageOnError = true

        head("App Similarity", "1.0")

        opt[String]('t', "theta") required() action {
            (t, cfg) => cfg.copy(theta = t)
        } text "document-topic file Path."

        opt[String]('v', "topicVector") required() action {
            (t, cfg) => cfg.copy(topicVector = t)
        } text "topic vector file Path."

        opt[String]('a', "appName") required() action {
            (a, cfg) => cfg.copy(appName = a)
        } text "app name file Path."

        opt[String]('o', "output") required() action {
            (o, cfg) => cfg.copy(output = o)
        } text "Result Output Path."

        help("help") text "prints this usage text"
    }

    def main(args: Array[String]): Unit = {
        execute(args)
    }

    def execute(args: Array[String]) = {
        for(config <- configParser.parse(args, Config(null, null, null, null))) {

            val topicVector = Source.fromFile(config.topicVector)
                .getLines()
                .map{ line =>
                    val arr = line.split(" ")
                        .map(_.toDouble)
                    DenseVector.apply(arr)
                }
                .toSeq

            val documentTopic = Source.fromFile(config.theta)
                .getLines()
                .map{ line =>
                    val topic = line.split(" ")
                        .map(_.toDouble)
                    topic.zipWithIndex.map{ case (t, i) =>
                        t * topicVector(i)
                    }.reduce(_ + _)
                }

            val appNameMap = Source.fromFile(config.appName)
                .getLines()
                .map{ line =>
                    line.split("\t")(1)
                }
                .toSeq

            val bw = new BufferedWriter(new FileWriter(config.output))

            documentTopic
                .zipWithIndex
                .foreach{ case(r, i) =>
                    val result = r.toArray
                        .map(a => f"$a%6f")
                        .mkString(" ")

                    bw.write(appNameMap(i) + " " + result + "\n")
                }
            bw.flush()
            bw.close()
        }
    }

}
