import java.io.{BufferedWriter, FileWriter}

import org.ansj.splitWord.analysis.ToAnalysis

import scala.io.Source
import scala.collection.JavaConversions._

/**
  * Created by cailiming on 17-12-19.
  */
object GenEcomProduct {
    def main1(args: Array[String]): Unit = {
        val input = Source.fromFile("/home/mi/Documents/Resources/croups/电商/categories.txt")
            .getLines()
            .toSeq

        val needProduct = Set("女装", "童装", "男装", "孕婴服饰", "运动户外")

        val ans = input
            .filter { line =>
                val f = line.split(";").head.trim

                needProduct.contains(f)
            }
            .flatMap { line =>
                line.split(";").flatMap(_.trim.split("/").map { a =>
                    a.trim.replace("男童", "").replace("男款", "").replace("女款", "").replace("女童", "").replace("中性款", "")
                        .replace("女式", "").replace("男式", "").replace("婴幼儿", "").replace("婴儿", "").replace("中性", "")
                })
            }
            .distinct

        val bw = new BufferedWriter(new FileWriter("/home/mi/Desktop/clothes_product.txt"))
        ans.foreach(a => bw.write(a + "\n"))

        bw.flush()
        bw.close()
    }

    def main2(args: Array[String]): Unit = {
        val bw = new BufferedWriter(new FileWriter("/home/mi/Desktop/product_clothing_outdoors.txt"))

        Source.fromFile("/home/mi/Desktop/product_clothing_sport.txt")
            .getLines()
            .map(_.trim.toUpperCase)
            .toSeq
            .distinct
            .foreach(l =>
                bw.write(l + "\n")
            )

        bw.flush()
        bw.close()
    }

    def main3(args: Array[String]): Unit = {
        val bw = new BufferedWriter(new FileWriter("/home/mi/Desktop/color_clothing_outdoors.txt"))

        Source.fromFile("/home/mi/Desktop/color.txt")
            .getLines()
            .flatMap { line =>
                if(line.endsWith("色") && line.length >= 3) {
                    Seq(line, line.substring(0, line.length - 1))
                } else {
                    Seq(line)
                }
            }
            .toSeq
            .distinct
            .foreach(l =>
                bw.write(l + "\n")
            )

        bw.flush()
        bw.close()
    }

    def main4(args: Array[String]): Unit = {
        val ans = Source.fromFile("/home/mi/Desktop/brand_clothing_outdoors.txt")
            .getLines()
            .filter(_.trim.length <= 27)
            .flatMap { line =>
                val split = line.split("（")
                if(split.last.contains("）"))
                    Seq(split.head, split.last.replace("）", "")).map(_.trim.toLowerCase)
                else
                    Seq(split.head).map(_.trim.toLowerCase)
            }
            .toSeq
            .distinct
            .filter { line =>
                line.length <= 10 || line.map(c => (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')).reduce(_ && _)
            }

        val bw = new BufferedWriter(new FileWriter("/home/mi/Desktop/brand_clothing_outdoors_processed.txt"))

        ans.foreach(l => bw.write(l + "\n"))

        bw.flush()
        bw.close()
    }

    def main5(args: Array[String]): Unit = {
        val ans1 = Source.fromFile("/home/mi/Desktop/product_clothing_outdoors.txt")
            .getLines()
            .toSeq

        val ans2 = Source.fromFile("/home/mi/Documents/Resources/croups/电商/title-ner/product_clothing_outdoors.txt")
            .getLines()
            .toSeq

        val ans = ans1.union(ans2).map(_.trim.toLowerCase).distinct

        val bw = new BufferedWriter(new FileWriter("/home/mi/Desktop/product_clothing_outdoors_processed.txt"))

        ans.foreach(l => bw.write(l + "\n"))

        bw.flush()
        bw.close()
    }

    def main(args: Array[String]): Unit = {
        val input1 = Source.fromFile("/home/mi/Documents/Resources/croups/电商/tbProductName.txt")
            .getLines()
            .toSeq
        val input2 = Source.fromFile("/home/mi/Documents/Resources/croups/电商/vipProductName.txt")
            .getLines()
            .toSeq
        val input = input1.union(input2)

        val bw = new BufferedWriter(new FileWriter("/home/mi/Documents/Resources/croups/电商/productNameWord2VecCroups.txt"))

        input
            .foreach { line =>
                val text = line.replace("\t", " ").replace(" ", "#").trim.toLowerCase()
                val seged = ToAnalysis.parse(text)
                    .getTerms

                val ans = seged
                    .flatMap { term =>
                        if(term.getNatureStr == "en") {
                            Seq(term.getName)
                        } else if(term.getNatureStr == "m") {
                            Seq(processNum(term.getName))
                        } else {
                            term.getName.map(c => c + "")
                        }
                    }
                    .mkString(" ")
                bw.write(ans + "\n")
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
