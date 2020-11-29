package com.ds.practise.nlp.analyzer

import com.ds.practise.nlp.common.{AppProperties, Constant}
import com.ds.practise.nlp.model.Email
import org.apache.commons.text.WordUtils
import org.apache.spark.ml.feature.{CountVectorizer, RegexTokenizer, StopWordsRemover}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.mllib.clustering.{LDA, OnlineLDAOptimizer}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}

/**
 * Class which defines the abtract behaviour of ideal chat analyzer should have
 * along with some default implementations.
 * @param props
 */
abstract class BaseChatAnalyzer(props: AppProperties) {

  /**
   * load data from any format as Spark DataFrame
   * @param spark - Spark Session object
   * @return
   */
  def loadData( spark: SparkSession) : DataFrame

  /**
   * Parse and formats the input data to create final resulting data
   * @param df - Input data frame
   * @param outputColumns - Expected list of output columns
   * @return
   */
  def formatOutputColumns(df: DataFrame, outputColumns: Array[Column]): DataFrame

  /**
   * Logic to publish email
   * @param emails - List of email object
   */
  def publishEmail(emails: Array[Email]): Unit

  /**
   * This method holds Data Pipeline flow
   * to process chat messages and get Email objects out of it.
   * @param spark - Spark Session
   * @return
   */
  def processTradeChats( spark: SparkSession): Array[Email] = {

    var df  = loadData( spark)

    df = chatTextToLowerCase(df)

    val outputColumns = props.get(Constant.OUTPUT_COLUMNS).split(",").map(col)

    df = formatOutputColumns(df, outputColumns)

    df = tokenizeInputText(df)

    df = filterByMinNoOfWord(df)

    df = removeStopWords(df)

    df = keywordSearch(df)

    df = getLargeContentForEachAuthor(df)

    import spark.implicits._
    val ds: Dataset[Email] = df.select(outputColumns: _*).as[Email]
    ds.collect()
  }

  /**
   * Convert chat message characters to lower case
   * @param df
   * @return
   */
  def chatTextToLowerCase(df: DataFrame): DataFrame = {
    df.withColumn("selftext", lower(col("selftext")))
  }

  /**
   * Filter dataframe to get the records with chat messages with most number of words for each author.
   * @param df
   * @return
   */
  def getLargeContentForEachAuthor(df : DataFrame): DataFrame = {
    val byNoOfWords = Window.partitionBy(col("from") ).orderBy(col("no_of_words"))
    df.withColumn("rank", rank over byNoOfWords)
      .filter(col("rank") > 1)
      .orderBy(col("no_of_words").desc)
      .limit(1000)
  }

  /**
   * Filter records if any of the topic input keywords matches to the tokens
   * @param df
   * @return
   */
  def keywordSearch(df : DataFrame) : DataFrame = {
    val topicRelatedKeywords = props.get(Constant.TOPIC_RELATED_WORDS).split(",")
    val keywordSize = topicRelatedKeywords.size
    df.filter(size(array_except(typedLit(topicRelatedKeywords), col("filteredTokens"))) =!= keywordSize)
  }

  /**
   * Split the big text into tokens/words
   * @param df
   * @return
   */
  def tokenizeInputText(df : DataFrame): DataFrame = {
    val tokenizer = new RegexTokenizer()
      .setPattern("[\\W_]+")
      .setInputCol("selftext")
      .setOutputCol("tokens")

    tokenizer.transform(df)
  }

  /**
   * Filter records which has more number of words than the threshold value.
   * @param df
   * @return
   */
  def filterByMinNoOfWord(df : DataFrame) : DataFrame = {
    df.withColumn("no_of_words",size(col("tokens")))
      .filter(col("no_of_words") > props.get(Constant.MIN_WORDS_REQUIRED).toInt)
  }

  def time[R](block: => R): Long = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")
    (t1 - t0)
  }

  /**
   * This method does the text wrapping
   * @param text - Unformatted text
   * @return
   */
  def formatted(text: String): String = {
    props.get(Constant.EMAIL_MAX_LINE_LENGTH).toInt
    WordUtils.wrap(text,100,"\n\r",false)
  }

  /**
   * Remove the stop words from the tokens to reduce the noise from the data.
   * Iterative process extra stop keywords list will change with the input
   * @param df
   * @return
   */
  def removeStopWords(df: DataFrame): DataFrame = {
    val stopwordsFromConfig = props.get(Constant.STOP_WORDS).split(",") ++
      props.get(Constant.EXTRA_STOP_WORDS).split(",") ++
      (0 to 50).map(_.toString())
    val locale = props.get(Constant.CHAT_LOCALE)
    val remover = new StopWordsRemover()
      .setLocale(locale)
      .setStopWords(stopwordsFromConfig) // Load stop words from properties file
      .setInputCol("tokens")
      .setOutputCol("filteredTokens")

    remover.transform(df)
      .withColumn("id", monotonically_increasing_id())
  }

  /**
   * Get frequent terms used in the all chats combined
   * @param oldDf
   * @param spark
   */
  def getTopTermsFromEachTopic(oldDf : DataFrame,spark : SparkSession): Unit = {
    import spark.implicits._

    val vectorizerOutColumns = "features"

    val vectorizer = new CountVectorizer()
      .setInputCol("filteredTokens")
      .setOutputCol(vectorizerOutColumns)
      .setVocabSize(10000)
      .setMinDF(5)
      .fit(oldDf)

    val countVectors = vectorizer.transform(oldDf).select("id", vectorizerOutColumns)

    val lda_countVector : RDD[(Long, Vector)] = countVectors
      .map(row => {
        (row.getAs[Long]("id"),Vectors.fromML(row.getAs[SparseVector](vectorizerOutColumns)))
      }).rdd

    val lda = new LDA()
      .setOptimizer(new OnlineLDAOptimizer().setMiniBatchFraction(0.8))
      .setK(40)
      .setMaxIterations(3)
      .setDocConcentration(-1) // use default values
      .setTopicConcentration(-1) // use default values

    val ldaModel = lda.run(lda_countVector)

    // Review Results of LDA model with Online Variational Bayes
    val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 20)
    val vocabList = vectorizer.vocabulary
    val topics : Array[Array[(String, Double)]] = topicIndices.map { case (terms, termWeights) =>
      terms.map(vocabList(_)).zip(termWeights)
    }

    println(s"==========")
    topics.flatMap( topic => topic.map( term => term._1)).distinct
      .foreach(println(_))
  }


}
