package com.ds.practise.nlp.analyzer

import java.io.{File, PrintWriter}

import com.ds.practise.nlp.common.{AppProperties, Constant}
import com.ds.practise.nlp.model.Email
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

/**
 * This class give concrete logic about
 * how the input data should be consumed and how it should be published
 * @param props - Application properties object
 */
class TradeChatAnalyzer(props: AppProperties) extends BaseChatAnalyzer(props: AppProperties) {

  val logger: Logger = LoggerFactory.getLogger(classOf[TradeChatAnalyzer])

  /**
   * This where the code to push email should go for now pushing data to a simple text file.
   * @param emails - List of email object
   */
  override def publishEmail(emails: Array[Email]): Unit = {
    val writer = new PrintWriter(new File(s"${props.get(Constant.OUTPUT_PATH)}/emails.txt"))
    emails.foreach(email => {
      val sb: String = formatEmail(email)
      writer.write(sb)
    })
    writer.close()
  }

  /**
   * genrating formatted content out of Email object
   * @param email - Email object
   * @return
   */
  def formatEmail(email: Email): String = {
    val sb = new StringBuffer()
    sb
      .append("Message-ID: ")
      .append(email.messageId).append("\n\r")
      .append("From: ")
      .append(email.from).append("\n\r")
      .append("To: user@behavox.com").append("\n\r")
      .append("Subject: ")
      .append(email.subject).append("\n\r")
      .append("Date: ")
      .append(email.date).append("\n\r")
      .append("Mime-Version: 1.0").append("\n\r")
      .append("Content-Transfer-Encoding: 7bit").append("\n\r")
      .append("Content-Type: text/plain; charset=utf-8").append("\n\r")
      .append(formatted(email.selftext)).append("\n\r")
      .append("--------------------------------------").append("\n\r")
    sb.toString
  }

  /**
   * Implementation of how to mapinput and output columns/fields
   * @param df - Input data frame
   * @param outputColumns - Expected list of output columns
   * @return
   */
  def formatOutputColumns(df: DataFrame, outputColumns: Array[Column]): DataFrame = {
    df.withColumn("messageId", concat(col("created_utc"), lit("@"), col("domain")))
      .withColumn("from", concat(col("author"), lit("@"), col("domain")))
      .withColumnRenamed("title", "subject")
      .withColumn("date", date_format(from_unixtime(col("created_utc")), "EEE, d MMM yyyy HH:mm:ss xxxxx"))
      .select(outputColumns: _*)
  }

  /**
   *
   * @param spark - Spark Session object
   * @return
   */
  override def loadData(spark: SparkSession) : DataFrame = {
    val inputColumns = props.get(Constant.INPUT_COLUMNS).split(",").map(col)

    val mandatoryColumnsWhereClause: Column = inputColumns.map(_.isNotNull).reduce((col1, col2) => col1 && col2)

    val extraFilterOperations = extraFilters()

    val df = spark.read.json(props.get(Constant.INPUT_FILE_PATH))
      .select(inputColumns: _*)
      .where(mandatoryColumnsWhereClause
        && extraFilterOperations)
    df
  }

  /**
   * Reducing noise by skpiing rows with,
   * 1. [deleted] author
   * 2. empty chats
   * @return
   */
  def extraFilters(): Column = {
    col("author").notEqual("[deleted]") && col("selftext") =!= ""
    //          && $"domain".notEqual("self.wallstreetbets")
  }
}


