package com.ds.practise.nlp.entry

import com.ds.practise.nlp.analyzer.TradeChatAnalyzer
import com.ds.practise.nlp.common.AppProperties
import com.ds.practise.nlp.model.Email
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

/**
 * Entry class for the Chat processing
 */
object Main {

  val logger: Logger = LoggerFactory.getLogger(Main.getClass)

  def main(args: Array[String]): Unit = {

      Try(args(0)) match {
      case Failure(_) => {
        logger.error("Insufficient parameters. Please pass config file path.")
      }
      case Success(configFilePath) => {
        logger.info(s"Loading properties from file path=${configFilePath}")
        val props: AppProperties = new AppProperties(Seq(configFilePath))

        val spark : SparkSession = SparkSession.builder().getOrCreate()
        val chatAnalyzer = new TradeChatAnalyzer(props)
        val emails : Array[Email] = chatAnalyzer.processTradeChats(spark)
        chatAnalyzer.publishEmail(emails)
      }
    }
  }



}
