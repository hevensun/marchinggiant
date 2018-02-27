import org.ansj.splitWord.analysis.ToAnalysis

/**
  * Created by cailiming on 17-12-26.
  */
object SegTest {
    def main(args: Array[String]): Unit = {
        val ans = ToAnalysis.parse("首席名模店铺 2017秋季新款宽松白三杠运动风韩版长袖卫衣t恤衫男")
        println(ans)
    }
}
