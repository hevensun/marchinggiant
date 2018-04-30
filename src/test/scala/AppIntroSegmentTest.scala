import com.xiaomi.ad.keyword.marchinggiant.app.BaiduAppParser
import com.xiaomi.ad.keyword.marchinggiant.appsimilarity.AppIntroSegment
import com.xiaomi.ad.matrix.qu.keywordextraction.ExtraCoreWords
import com.xiaomi.ad.matrix.qu.tokenizer.impl.MiTokenizer
import com.xiaomi.ad.matrix.qu.wordnormalizer.WordNormalizer
import org.apache.spark.SparkConf

/**
  * Created by cailiming on 17-7-18.
  */
object AppIntroSegmentTest {
    def main(args: Array[String]): Unit = {
//        AppIntroSegment.execute1(args, new SparkConf().setMaster("local[3]"))
//        BaiduAppParser.execute(args)
//        val name = "轩辕传奇-鬼吹灯活动开启"
//        val result = name.toLowerCase.map{ categoryName.txt =>
//            val t1 = categoryName.txt >= 0x4e00 && categoryName.txt <= 0x9fbb
//            val t2 = categoryName.txt >= '0' && categoryName.txt <= '9'
//            val t3 = categoryName.txt >= 'a' && categoryName.txt <= 'z'
//            t1 || t2 || t3
//        }.reduce(_ && _) && name.map(categoryName.txt => categoryName.txt >= 0x4e00 && categoryName.txt <= 0x9fbb).reduce(_ || _)
//        println(result)

//        ExtraCoreWords.extraCoreWords("轩辕传奇-鬼吹灯活动开启").foreach(println)
        MiTokenizer.segment("北京市怀柔县参试学生普遍感觉第四节课饥饿感消失了，四川省江油市华丰中学选用豆奶和复合营养素后，试验组男生的贫血率下降13个百分点，而对照组只降低0．44个百分点。").foreach(println)
    }
}
