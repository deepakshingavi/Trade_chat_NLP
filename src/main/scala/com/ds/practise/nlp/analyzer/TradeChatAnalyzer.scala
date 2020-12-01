package com.ds.practise.nlp.analyzer

import java.io.{BufferedOutputStream, File, FileOutputStream}
import java.util.Date

import com.ds.practise.nlp.common.{AppProperties, Constant}
import com.ds.practise.nlp.model.{CustomMimeMessage, Email}
import javax.mail.internet.{InternetAddress, MimeBodyPart, MimeMessage, MimeMultipart}
import javax.mail.{Message, MessagingException, Session}
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
   * Mime email object are dumped to eml file
   * @param emails - List of email object
   */
  override def publishEmail(emails: Array[Email]): Array[MimeMessage] = {
    val fos = new FileOutputStream(new File(s"${props.get(Constant.OUTPUT_PATH)}/outputDump.eml"))
    val bos = new BufferedOutputStream(fos)
    val mimeVersion = props.get(Constant.DEFAULT_MIME_VERSION)
    val contentType = props.get(Constant.DEFAULT_CONTENT_TYPE)
    val contentTransferEncoding = props.get(Constant.DEFAULT_CONTENT_TRANSFER_ENCODING)
    val defaultToUser = props.get(Constant.DEFAULT_TO_USER)

    val mimeMsgs : Array[MimeMessage] = emails.map(email => {
      val msg = createMessage(email,defaultToUser,mimeVersion,contentType,contentTransferEncoding)
      msg.writeTo(bos)
      msg
    })
    fos.close()
    mimeMsgs
  }

  /**
   * Creates Mime email message object
   * @param email - Email object
   * @param to
   * @param mimeVersion
   * @param contentType
   * @param contentTransferEncoding
   * @throws
   * @return
   */
  @throws[MessagingException]
  def createMessage(email: Email, to: String, mimeVersion:String,contentType:String,contentTransferEncoding:String): MimeMessage = {
    val session : Session = null
    val msg = new CustomMimeMessage(session,email.messageId,mimeVersion,contentType,contentTransferEncoding)

    msg.setSentDate(new Date(email.created_utc*1000))
    msg.setFrom(new InternetAddress(email.from))
    msg.setRecipient(Message.RecipientType.TO, new InternetAddress(to))
    msg.setSubject(email.subject)
    val body = new MimeBodyPart()
    body.setText(email.selftext)
    msg.setContent(new MimeMultipart(body))
    msg.saveChanges()
    msg
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
//      .withColumn("date", date_format(from_unixtime(col("created_utc")), "EEE, d MMM yyyy HH:mm:ss xxxxx"))
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


