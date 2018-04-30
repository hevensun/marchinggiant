import java.io.{BufferedWriter, File, FileWriter}

import org.ansj.splitWord.analysis.ToAnalysis

import scala.io.Source
import scala.collection.JavaConversions._

/**
  * Created by cailiming on 18-3-5.
  */
object BaiduInputDic {
    def main(args: Array[String]): Unit = {
        val outNature = Set(
            "m",
            "q",
            "l",
            "a",
            "e",
            "r",
            "c",
            "null"
        )

        val ans = ToAnalysis.parse("西凤").getTerms
        println(ans)

//        val bw = new BufferedWriter(new FileWriter(new File("/home/mi/Desktop/baidu-input-dic.txt")))
//
//        Source.fromFile("/home/mi/Desktop/cn_ice_features.tsv")
//            .getLines()
//            .filter { l =>
//                val text = l.split("\t").head
//                val terms = ToAnalysis.parse(text)
//                    .getTerms
//
//                (terms.size() > 1 && terms.head.getNatureStr != "null") || !outNature.contains(terms.head.getNatureStr)
//            }
//            .foreach { l =>
//                val text = l.split("\t").head
//                val idf = l.split("\t").last
//                bw.write(text + "\t" + idf + "\n")
//            }
//
//        bw.flush()
//        bw.close()
    }

    def main1(args: Array[String]): Unit = {
        val outNature = Set(
            "m",
            "q",
            "l",
            "a",
            "e",
            "r",
            "c",
            "null"
        )

        Source.fromFile("/home/mi/Desktop/cn_ice_features.tsv")
            .getLines()
            .filter { l =>
                val text = l.split("\t").head
                val terms = ToAnalysis.parse(text)
                    .getTerms

//                !(terms.size() > 1 || !outNature.contains(terms.head.getNatureStr))
                terms.head.getNatureStr == "null"
            }
            .foreach { l =>
                val text = l.split("\t").head
                println(text)
            }
    }
}
