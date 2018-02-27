import java.io.{BufferedWriter, FileWriter}

import scala.io.Source

/**
  * Created by cailiming on 18-1-4.
  */
object PrepareAppNameDict {
    def main(args: Array[String]): Unit = {
        val bw = new BufferedWriter(new FileWriter("/home/mi/Documents/Resources/croups/app-dict/appName.txt"))

        Source.fromFile("/home/mi/Documents/Resources/croups/app-dict/app_name.txt")
            .getLines()
            .filter { line =>
                line.split("\t").last.toInt >= 50
            }
            .foreach { line =>
                val split = line.split("\t")
                val name = split.head
                val cnt = split.last.toInt / 1000
                bw.write(s"$name\tnapp\t${Math.max(1, cnt)}\n")
            }

        bw.flush()
        bw.close()
    }
}
