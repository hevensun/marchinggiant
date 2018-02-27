import java.io.{BufferedWriter, File, FileWriter}

import scala.io.Source
import scala.util.Try

/**
  * Created by cailiming on 18-2-6.
  */
object GoogleCategoryExpansion {
    def main(args: Array[String]): Unit = {
        val nameIdMap = Source.fromFile("/home/mi/Documents/Resources/CNICE_catelist_AllName.txt")
            .getLines()
            .map { line =>
                val split = line.split("\t", 3)
                split(1) -> split(0)
            }
            .toMap

        val bw = new BufferedWriter(new FileWriter(new File("/home/mi/Desktop/google-category-expansion.txt")))

        nameIdMap
            .map { case(name, id) =>
                val splits = name.split("/")
                val ids = splits
                    .zipWithIndex
                    .map { case(curName, index) =>
                        val preName = Try((0 until index).map(i => splits(i)).mkString("/")).getOrElse("")
                        nameIdMap.getOrElse(preName, "")
                    }
                    .filter(_.nonEmpty)
                id -> ids
            }
            .foreach { case(id, ids) =>
                val line = s"$id\t${Try(ids.mkString(" ")).getOrElse("")}\n"
                bw.write(line)
            }

        bw.flush()
        bw.close()
    }
}
