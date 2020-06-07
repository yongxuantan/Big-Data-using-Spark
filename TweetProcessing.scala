import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.{Tokenizer, StopWordsRemover, HashingTF, StringIndexer}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.mllib.evaluation.MulticlassMetrics


object TweetProcessing {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("TweetProcessing").getOrCreate()

    // Load the text file
    val df = spark.read.option("header","true").option("inferSchema","true").option("delimiter",",").csv(args(0))

    var str = ""

    // Remove rows where the text field is null.
    val sentiment = df.filter("text is not null").select("tweet_id", "airline_sentiment", "text").toDF("id", "airline_sentiment", "text")

    // Transform the text column into words by breaking down the sentence into words.
    val tokenizer = new Tokenizer()
      .setInputCol("text")
      .setOutputCol("words")

    // Remove stop-words from the text column.
    val remover = new StopWordsRemover()
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("filtered")

    // Convert words to term-frequency vectors.
    val hashingTF = new HashingTF()
      .setInputCol(remover.getOutputCol)
      .setOutputCol("features")

    // Convert the label to numeric format.
    val indexer = new StringIndexer().setInputCol("airline_sentiment").setOutputCol("label")

    // Create a logistic regression classification model.
    val lr = new LogisticRegression().setMaxIter(10)

    // Create a pipeline of the above steps
    val pipeline = new Pipeline().setStages(Array(tokenizer, remover, hashingTF, indexer, lr))

    // Create a ParameterGridBuilder for parameter tuning.
    val paramGrid = new ParamGridBuilder()
      .addGrid(hashingTF.numFeatures, Array(10, 100, 1000))
      .addGrid(lr.regParam, Array(0.1, 0.01))
      .build()

    // Use the Cross-Validator object for finding the best model parameters.
    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new MulticlassClassificationEvaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3)

    // Split the dataset to train and test
    val Array(train, test) = sentiment.randomSplit(Array(0.8, 0.2), seed = 11L)

    // Train model on the given dataset.
    val cvModel = cv.fit(train)

    // Test model on the given dataset.
    val predictionAndLabels = cvModel.transform(test).select("prediction", "label")

    // Instantiate metrics object.
    val predictionAndLabels_RDD = predictionAndLabels.rdd.map(x => (x.getAs[Double](0), x.getAs[Double](1)))
    val metrics = new MulticlassMetrics(predictionAndLabels_RDD)

    // Output classification evaluation metrics
    val accuracy = metrics.accuracy
    val labels = metrics.labels


    str=str+"Summary Statistics\n"
    str=str+s"Accuracy = $accuracy \n"
    str=str+"\n"
    str=str+"Precision by label"
    labels.foreach { l =>
      str=str+s"Precision($l) = " + metrics.precision(l) + "\n"
    }
    str=str+"\n"
    str=str+"Recall by label\n"
    labels.foreach { l =>
      str=str+s"Recall($l) = " + metrics.recall(l) + "\n"
    }
    str=str+"\n"
    str=str+"False positive rate by label\n"
    labels.foreach { l =>
      str=str+s"FPR($l) = " + metrics.falsePositiveRate(l) + "\n"
    }

    spark.sparkContext.parallelize(Seq(str))
      .saveAsTextFile(args(1))

  }
}
