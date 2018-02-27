import com.xiaomi.matrix.cat.{CatModelConf, CatModelFacade}

/**
  * Created by cailiming on 18-2-9.
  */
object LocalGoogleCategory {
    def main(args: Array[String]): Unit = {
        val conf = CatModelConf.builder
            .setModelPath("model", "/home/mi/Develop/data/google-category-model/model.bin")
            .setModelPath("offset", "/home/mi/Develop/data/google-category-model/offset.bin")
            .setTaxonomy("google")
            .build

        val model = CatModelFacade.getOrInit(conf)

        val text = "美丽说 时尚购物,电商 美丽说（北京）网络科技有限公司,内容提要：  美丽说 一个让白领买到全球时尚好物的APP  白领的品质生活  就是比原来讲究一点  挑喜欢的品牌、看最新的搭配,一个让白领买到全球时尚好物的APP"

        val ans = model.predict(text).mkString(" ")

        println(ans)
    }
}
