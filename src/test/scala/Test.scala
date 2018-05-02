import java.io.{BufferedWriter, File, FileWriter}
import java.text.SimpleDateFormat
import java.util.Date

import com.twitter.scalding.RichDate
import org.joda.time.DateTime

import scala.io.Source
import scala.util.Random

/**
  * Created by cailiming on 17-9-7.
  */
object Test {
    def main(args: Array[String]): Unit = {
        val bw = new BufferedWriter(new FileWriter(new File("/home/mi/Desktop/cailiming.txt")))

        (0 to 100000)
            .foreach { i =>
                bw.write("吃饭了吗\n")
            }

        bw.flush()
        bw.close()
    }

    def main12(args: Array[String]): Unit = {
        val file = new File("/home/mi/Desktop/tmp2/sample_submission.csv")
        file.createNewFile()
        val bw = new BufferedWriter(new FileWriter(file))
        val r = new Random()
        bw.write("instance_id,proba\n")

        Source.fromFile("/home/mi/Desktop/tmp2/answer_all.csv")
            .getLines()
            .drop(1)
            .foreach{ l =>
                val splits = l.split(",")
                val key = splits.head
                val v = r.nextDouble()

                val ans = f"$key,0.5"

                bw.write(ans + "\n")
            }

        bw.flush()
        bw.close()
    }

    def main3(args: Array[String]): Unit = {
        val ans = new DateTime().toString("HHmm")
//        val ans = new SimpleDateFormat("HHmm").format(date)
        println(ans)
    }

    def main4(args: Array[String]): Unit = {
        val appId = Source.fromFile("/home/mi/Desktop/contest/city")
            .getLines()

        val bw = new BufferedWriter(new FileWriter(new File("/home/mi/Desktop/city")))

        Random.shuffle(appId).zipWithIndex
            .foreach{ case (appId, index) =>
                bw.write(appId + "\t" + index + "\n")
            }

        bw.flush()
        bw.close()
    }

    def main1(args: Array[String]): Unit = {
        val file = new File("/home/mi/Desktop/tmp/32_with_label")
        file.createNewFile()
        val bw = new BufferedWriter(new FileWriter(file))

        Source.fromFile("/home/mi/Desktop/tmp/20170815_with_label")
            .getLines()
            .map{ l =>
                val splits = l.split("\t")
                "32" + getInstanceId(splits.head.substring(8, splits.head.length)) + "\t" + splits(1)
            }
    }

    def getInstanceId(instance: String) = {
        val zeroNum = 7 - instance.length
        val resultBuilder = new StringBuilder
        (0 until zeroNum)
            .foreach(a => resultBuilder.append("0"))
        resultBuilder.append(instance)
        resultBuilder.toString
    }

    def main21(args: Array[String]): Unit = {
        val predict = Source.fromFile("/home/mi/Desktop/tmp/sample_submission.csv")
            .getLines()
            .drop(1)
            .map{ l =>
                val splits = l.split(",")
                splits(1).toDouble
            }
            .toSeq
        println(predict.length)

        val ans = Source.fromFile("/home/mi/Desktop/tmp/answer_all.csv")
            .getLines()
            .drop(1)
            .map{ l =>
                val splits = l.split(",")
                splits(1).toDouble
            }
            .toSeq

        println(ans.length)

        val pa = predict.zip(ans)

        println(pa.length)
//
        val logLoss = logloss(pa)
        println(logLoss)
    }

    def logloss(predictionAndLabels: Seq[(Double, Double)]) = {
        val loss = predictionAndLabels
            .map{ case (prediction, label) =>
                if(label > 0)
                    -Math.log(minMax(prediction))
                else
                    -Math.log(minMax(1 - prediction))
            }
            .sum
        loss / predictionAndLabels.length
    }

    def minMax(i: Double) = {
        if(i < 0.000000000000001) 0.000000000000001
        else i
    }
}
