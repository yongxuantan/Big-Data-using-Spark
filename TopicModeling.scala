import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector

import scala.collection.mutable
import org.apache.spark.mllib.clustering.{DistributedLDAModel, LDA}
import org.apache.spark.rdd.RDD

object TopicModeling {

  def main(args: Array[String]): Unit = {

    val inpath = args(0)
    val outpath = args(1)

    //Create a SparkContext to initialize Spark
    val conf = new SparkConf().setAppName("TopicModeling")
    val sc = new SparkContext(conf)

    var str = ""

    // bring data into an RDD
    val input = sc.wholeTextFiles(inpath).map(_._2)

    // get set of stop words
    val stopWordSet = StopWordsRemover.loadDefaultStopWords("english").toSet

    // split into words
    // only keep words longer than 3 characters and not a stop word
    val tokenized: RDD[Seq[String]] = input.map(_.toLowerCase.split("\\s"))
      .map(_.filter(_.length > 3).filter(token => !stopWordSet.contains(token))
        .filter(_.forall(java.lang.Character.isLetter)))

    // Choose the vocabulary.
    //   termCounts: Sorted list of (term, termCount) pairs
    val termCounts: Array[(String, Long)] =
    tokenized.flatMap(_.map(_ -> 1L)).reduceByKey(_ + _).collect().sortBy(-_._2)

    //   vocabArray: Chosen vocab (removing common terms)
    val numStopwords = 20
    val vocabArray: Array[String] =
      termCounts.takeRight(termCounts.size - numStopwords).map(_._1)

    //   vocab: Map term -> term index
    val vocab: Map[String, Int] = vocabArray.zipWithIndex.toMap

    // Convert documents into term count vectors
    val documents: RDD[(Long, Vector)] =
      tokenized.zipWithIndex.map { case (tokens, id) =>
        val counts = new mutable.HashMap[Int, Double]()
        tokens.foreach { term =>
          if (vocab.contains(term)) {
            val idx = vocab(term)
            counts(idx) = counts.getOrElse(idx, 0.0) + 1.0
          }
        }
        (id, Vectors.sparse(vocab.size, counts.toSeq))
      }

    // Set LDA parameters
    val numTopics = 5
    val lda = new LDA().setK(numTopics).setMaxIterations(10)

    val ldaModel = lda.run(documents)

    // Print topics, showing top-weighted 10 terms for each topic.
    val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 10)
    topicIndices.foreach { case (terms, termWeights) =>
      str = str + "TOPIC:\n"
      terms.zip(termWeights).foreach { case (term, weight) =>
        str = str + s"${vocabArray(term.toInt)}\t$weight\n"
      }
      str = str + "\n"
    }

    // save most important topics
    sc.parallelize(Seq(str))
      .saveAsTextFile(outpath)
  }
}
