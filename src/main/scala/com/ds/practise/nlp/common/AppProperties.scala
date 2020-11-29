package com.ds.practise.nlp.common

import java.io.{File, FileInputStream, IOException, InputStream}
import java.util.Properties

import org.slf4j.LoggerFactory

object AppProperties {

}

/**
 * Load and creates a Properties file object
 * @param propsFilePaths - Properties file path
 */
class AppProperties @throws[IOException]
(propsFilePaths: Seq[String]) extends Serializable {
  private val logger = LoggerFactory.getLogger(classOf[AppProperties])

  private val properties: Properties = new Properties
  try {
    propsFilePaths.map(new FileInputStream(_)).foreach( (inputStream : InputStream) => {
      val tempProps = new Properties()
      tempProps.load(inputStream)
      properties.putAll(tempProps)
    })
  }
  catch {
    case e: Exception =>
      logger.error("Application path=" + new File("").getAbsolutePath, e)
      throw e
  }

  def get(key: String): String = properties.getProperty(key).trim

}
