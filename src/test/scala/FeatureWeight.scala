import java.io.{BufferedWriter, FileWriter}

import org.apache.spark.sql.SparkSession

import scala.io.Source

/**
  * Created by cailiming on 17-7-17.
  */
object FeatureWeight {
    def main(args: Array[String]): Unit = {
        def buildStringPairFeature(name: String, featureBuilder: StringBuilder, startIndex: Int): Int = {
            var index = startIndex
            featureBuilder.append(s" $index,$name(levenshteinSimilarity)")
            index += 1
            featureBuilder.append(s" $index,$name(jaccardSimilarity-1)")
            index += 1
            featureBuilder.append(s" $index,$name(jaccardSimilarity-2)")
            index += 1
            featureBuilder.append(s" $index,$name(jaccardSimilarity-3)")
            index += 1
            featureBuilder.append(s" $index,$name(diceSorensenSimilarity-1)")
            index += 1
            featureBuilder.append(s" $index,$name(diceSorensenSimilarity-2)")
            index += 1
            featureBuilder.append(s" $index,$name(diceSorensenSimilarity-3)")
            index += 1
            featureBuilder.append(s" $index,$name(jaroSimilarity)")
            index += 1
            featureBuilder.append(s" $index,$name(jaroWinklerSimilarity)")
            index += 1
            featureBuilder.append(s" $index,$name(ngramSimilarity-1)")
            index += 1
            featureBuilder.append(s" $index,$name(ngramSimilarity-2)")
            index += 1
            featureBuilder.append(s" $index,$name(ngramSimilarity-3)")
            index += 1
            featureBuilder.append(s" $index,$name(overlapSimilarity-1)")
            index += 1
            featureBuilder.append(s" $index,$name(overlapSimilarity-2)")
            index += 1
            featureBuilder.append(s" $index,$name(overlapSimilarity-3)")
            index += 1
            featureBuilder.append(s" $index,$name(ratcliffObershelpSimilarity)")
            index += 1

            index
        }

        def buildWordsPairFeature(name: String, featureBuilder: StringBuilder, startIndex: Int) = {
            var index = startIndex
            featureBuilder.append(f" $index,$name(jaccardWrodsSimilarity)")
            index += 1
            featureBuilder.append(f" $index,$name(diceSorensenWrodsSimilarity)")
            index += 1
            index
        }

        def buildStringEqualFeature(name: String, featureBuilder: StringBuilder, startIndex: Int) = {
            featureBuilder.append(f" $startIndex,$name(stringEqual)")
            startIndex + 1
        }

        def buildCategoryFeature(name: String, featureBuilder: StringBuilder, startIndex: Int) = {
            var index = startIndex
            featureBuilder.append(f" $index,$name(categoryCosineSimilarity)")
            index += 1
            featureBuilder.append(f" $index,$name(categoryFirstNContainEqual-1)")
            index += 1
            featureBuilder.append(f" $index,$name(categoryFirstNContainEqual-2)")
            index += 1
            featureBuilder.append(f" $index,$name(categoryFirstNContainEqual-3)")
            index += 1
            index
        }

        def buildTopicFeature(name: String, featureBuilder: StringBuilder, startIndex: Int) = {
            var index = startIndex
            featureBuilder.append(f" $index,$name(topicCosineSimilarity)")
            index += 1
            featureBuilder.append(f" $index,$name(topicFirstNContainEqual-1)")
            index += 1
            featureBuilder.append(f" $index,$name(topicFirstNContainEqual-2)")
            index += 1
            featureBuilder.append(f" $index,$name(topicFirstNContainEqual-3)")
            index += 1
            index
        }

        def buildKeywordFeature(name: String, featureBuilder: StringBuilder, startIndex: Int) = {
            var index = startIndex
            featureBuilder.append(f" $index,$name(keywordConsineSimilarity)")
            index += 1
            featureBuilder.append(f" $index,$name(keywordFirstNContainEqual-1)")
            index += 1
            featureBuilder.append(f" $index,$name(keywordFirstNContainEqual-2)")
            index += 1
            featureBuilder.append(f" $index,$name(keywordFirstNContainEqual-3)")
            index += 1
            index
        }

        val featureBuilder = new StringBuilder
        val index0 = 1

        val index1 = buildStringPairFeature("displayName-str", featureBuilder, index0)
        val index2 = buildWordsPairFeature("displayName-words", featureBuilder, index1)

        val index3 = buildStringEqualFeature("level1CategoryName-equal", featureBuilder, index2)
        val index4 = buildStringEqualFeature("level2CategoryName-equal", featureBuilder, index3)

        val index5 = buildStringEqualFeature("publisherName-euqal", featureBuilder, index4)

        val index6 = buildStringPairFeature("introduction-str", featureBuilder, index5)
        val index7 = buildWordsPairFeature("introduction-words", featureBuilder, index6)

        val index8 = buildStringPairFeature("brief-str", featureBuilder, index7)
        val index9 = buildWordsPairFeature("brief-words", featureBuilder, index8)

        val index10 = buildWordsPairFeature("keywords", featureBuilder, index9)

        val index11 = buildKeywordFeature("introkeywords", featureBuilder, index10)

        val index12 = buildCategoryFeature("category-category", featureBuilder, index11)
        val index13 = buildCategoryFeature("intro-category", featureBuilder, index12)
        val index14 = buildCategoryFeature("keyword-category", featureBuilder, index13)

        val index15 = buildTopicFeature("lda", featureBuilder, index14)

        val allIndex = featureBuilder.toString()
        val weights = "0.3308564837543286,0.41747034557802476,0.15351118462289123,0.009286158365692875,0.6930495477832065,0.2726355053367907,0.019694288646394813,0.6078968362924029,0.6272987933638227,0.5946478447984618,0.23090453469292319,0.015927594214574573,0.8884601853725066,0.36057754950191734,0.02827238939107021,0.6812666733601879,0.08799550927912135,0.14253962731496855,0.6025554345250578,0.3541550840268827,0.06838575989534132,0.019831234267874708,0.16183647817382632,0.08391664481584003,0.01672248384385249,0.23404798826777426,0.1658066086652056,0.03934153825556163,-0.04010774870171034,-0.03126072773720158,0.18552584821151824,0.1280056812767535,0.029481380409395606,0.4606779534440023,0.30079670688853344,0.07235887938293876,0.050011211005440745,0.12775387002591668,0.24698836013695183,0.033423387663442126,0.07132143215374256,0.022335627016946687,2.171682692235003E-5,0.12980756779190913,0.04564167601011259,0.003820731223891658,0.1873248440342129,0.18639495294988284,0.11422783267199053,0.03977687101134673,0.002811918412320946,0.15609780234869353,0.05578155914338618,0.00559911803673351,0.10726956303103546,-0.12200529962892717,-0.10042314472538476,0.20398650748450708,0.3652672724637313,0.03092266792616217,-3.3074556397212737E-4,0.020765770459084763,0.15678255438947578,0.08193473745893912,0.21100000451442563,-0.0342484186288464,-0.062480583498520835,0.0030791503049653504,0.040188469055211305,0.11335713719685446,0.28028623779405065,5.2060377951310886E-5,0.2192569336197144,0.37269565129701426,0.5701505382901759,0.01363004359665086,0.0652175342220602,0.0466416637114383,0.11382741949585778"

        val bw = new BufferedWriter(new FileWriter("/home/mi/Develop/data/app-meta-info/app-similarity/init-model/feature-weights.csv"))
        val wt = weights.split(",")
        allIndex.trim.split(" ")
            .zipWithIndex
            .map{ case(str, i) =>
                str + "," + wt(i) + "\n"
            }
            .foreach{ l =>
                bw.write(l)
            }

        bw.flush()
        bw.close()
    }
}
