import java.io.{BufferedWriter, File, FileWriter}

import com.xiaomi.ad.matrix.qu.wordnormalizer.WordNormalizer

import scala.io.Source
import scala.util.Try

/**
  * Created by cailiming on 17-9-6.
  */
object AppInfo {
    val SPLITER = "#SPLITER#"

    def main3(args: Array[String]): Unit = {
        val file = new File("/home/mi/Desktop/app-info/result.txt")
        file.createNewFile()
        val bw = new BufferedWriter(new FileWriter(file))

        Source.fromFile("/home/mi/Desktop/app-info/app_info.txt")
            .getLines()
            .foreach{ l =>
                val splits = l.split(SPLITER)
                val packageName = splits(0)
                val appId = splits(1).toLong
                val level1CategoryName = Try(splits(3).replace("\t", "")).getOrElse("")
                val level2CategoryName = Try(splits(4).replace("\t", "")).getOrElse("")
                val keywords = Try(splits(8).replace("　", ";").replace("、", ";").replace(",", ";").replace("，", ";").replace(" ", ";").replace("\t", "")).getOrElse("")

                val result = packageName + "\t" + appId + "\t" + keywords + "\t" + level1CategoryName + "\t" + level2CategoryName
                bw.write(result + "\n")
            }

        bw.flush()
        bw.close()
    }

    def main1(args: Array[String]): Unit = {
        Source.fromFile("/home/mi/Desktop/app-info/result.txt").getLines()
            .foreach{ l =>
                println(l)
                val splits = l.split("\t")
                println(splits.head + "\t" + splits(1) + "\t" + splits(2) + "\t" + splits(3) + "\t" + splits(4))
            }
    }

    def main(args: Array[String]): Unit = {
        val file = new File("/home/mi/Desktop/app-out1.txt")
        file.createNewFile()
        val bw = new BufferedWriter(new FileWriter(file))

        Source.fromFile("/home/mi/Desktop/app-out.txt")
            .getLines()
            .foreach { l =>
                bw.write(WordNormalizer.normalize(l) + "\n")
            }

        bw.flush()
        bw.close()
    }
}
