import java.io.{BufferedWriter, File, FileWriter}

import scala.io.Source
import scala.util.Random

/**
  * Created by cailiming on 17-12-5.
  */
object Car_Category {
    def main1(args: Array[String]): Unit = {
        val bw = new BufferedWriter(new FileWriter(new File("/home/mi/Desktop/car_category.txt")))

        Source.fromFile("/home/mi/Documents/Resources/CNICE_catelist_AllName.txt")
            .getLines()
            .filter { line =>
                val split = line.split("\t")
                val str = split(1)
                (str.contains("交通") || str.contains("机动车") || str.contains("汽车")) && !str.contains("公交")
            }
            .foreach { line =>
                bw.write(line + "\n")
            }

        bw.flush()
        bw.close()
    }

    def main2(args: Array[String]): Unit = {
        val ans = Source.fromFile("/home/mi/Desktop/car-classify-1.txt")
            .getLines()
            .map { line =>
                val split = line.split("\t")
                (split.head, split(1), split(2).toDouble)
            }
            .toSeq
            .groupBy(_._1)
            .mapValues { vs =>
                val max = vs.maxBy(_._3)
                if(max._2 == "驾考摇号" && max._3 >= 0.9)
                    max
                else
                    vs.filter(_._2 != "驾考摇号").maxBy(_._3)
            }
            .values
            .filter(a => a._3 >= 0.69)
            .toSeq

        println(ans.length)

        ans.sortBy(-_._3).foreach { a =>
            println(a._1 + "\t" + a._2 + "\t" + a._3)
        }
    }

    def main(args: Array[String]): Unit = {
        val appPV = Source.fromFile("/home/mi/Desktop/app-pv")
            .getLines()
            .map { line =>
                val split = line.split("\t")
                split.head -> split.last.toInt
            }
            .toSeq

        val appCategory = Source.fromFile("/home/mi/Desktop/car_category_final.txt")
            .getLines()
            .filter(_.trim.nonEmpty)
            .map { line =>
                val split = line.split("\t")
                split.head -> (split(1), split(2).toDouble)
            }
            .toMap

        appPV
            .filter(a => appCategory.contains(a._1))
            .foreach { case(pn, pv) =>
                val cate = appCategory(pn)
                println(pn + "  " + pv + "  " + cate._1 + "  " + cate._2 )
            }
    }

    def main4(args: Array[String]): Unit = {
        val bw = new BufferedWriter(new FileWriter(new File("/home/mi/Desktop/car_category.txt")))

        Source.fromInputStream(Car_Category.getClass.getResourceAsStream("/car-core"))
            .getLines()
            .filter(_.trim.nonEmpty)
            .foreach { line =>
                val split = line.split("\t")
                if(split.length == 4)
                    bw.write(s"${split.head}\t${split(2)}\t${split(3)}\n")
                else
                    bw.write(s"${split.head}\t${split(1)}\t${split(2)}\n")
            }

        bw.flush()
        bw.close()
    }

    def main5(args: Array[String]): Unit = {
        val ans = Source.fromFile("/home/mi/Desktop/car-classify-2.txt")
            .getLines()
            .map { line =>
                val split = line.split("\t")
                (split.head, split(1), split(2).toDouble)
            }
            .toSeq
            .groupBy(_._1)
            .mapValues { vs =>
                val ordered = vs.sortBy(-_._3)

                val first = ordered.head
                val second = ordered(1)

                val ans = if(first._2 == "驾考摇号") {
                    if(first._3 >= 0.8) {
                        Seq(first)
                    } else {
                        Seq(vs.filter(_._2 != "驾考摇号").maxBy(_._3))
                    }
                } else {
                    if(first._3 >= 0.6)
                        Seq(first)
                    else {
                        if(second._2 == "驾考摇号") {
                            Seq(first)
                        } else {
                            if(first._3 - second._3 <= 0.1) {
                                Seq(first, second)
                            } else {
                                Seq(first)
                            }
                        }
                    }
                }

                ans
            }
            .values
            .flatMap(_.toSeq)
            .toSeq

        println(ans.length)

        ans.sortBy(-_._3).foreach { a =>
            println(a._1 + "\t" + a._2 + "\t" + a._3)
        }
    }

    def main6(args: Array[String]): Unit = {
        val apps = Source.fromFile("/home/mi/Desktop/car_category_fianl.txt")
            .getLines()
            .toSeq

        val shuffled = Random.shuffle(apps)

        shuffled.take(6)
            .foreach(println)
    }

    def main7(args: Array[String]): Unit = {
        val bw = new BufferedWriter(new FileWriter(new File("/home/mi/Desktop/car_category_fianl.txt")))

        val out = Source.fromFile("/home/mi/Desktop/need-2-check.txt")
            .getLines()
            .map(_.split("\t").head)
            .toSet

        Source.fromFile("/home/mi/Desktop/car_category.txt")
            .getLines()
            .foreach { line =>
                val pn = line.split("\t").head

                if(!out.contains(pn)) {
                    bw.write(line + "\n")
                }
            }

        bw.flush()
        bw.close()
    }
}
