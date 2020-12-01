package com.ds.practise.nlp.common

import org.apache.commons.text.WordUtils

/**
 * List of all constant value
 */
object Constant {

  val INPUT_FILE_PATH ="input.file.path"
  val OUTPUT_PATH ="output.folder.path"
  val STOP_WORDS ="stop.words"
  val EXTRA_STOP_WORDS ="extra.stop.words"
  val INPUT_FILE_FORMAT ="file.format"
  val CHAT_LOCALE= "chat.locale"
  val EMAIL_TO = "email.to"
  val MIN_WORDS_REQUIRED = "min.words.required"
  val TOPIC_RELATED_WORDS = "topic.related.words"
  val INPUT_COLUMNS = "input.columns"

  val OUTPUT_COLUMNS = "output.columns"

  val EMAIL_MAX_LINE_LENGTH = "email.max.line.length"

  val DEFAULT_MIME_VERSION = "default.mime.version"
  val DEFAULT_CONTENT_TYPE = "default.content.type"
  val DEFAULT_CONTENT_TRANSFER_ENCODING = "default.content.transfer.encoding"
  val DEFAULT_TO_USER= "default.to.user"
  val MESSAGE_ID = "Message-ID"
  val MIME_VERSION = "MIME-Version"
  val CONTENT_TRANSFER_ENCODING = "Content-Transfer-Encoding"
  val CONTENT_TYPE = "Content-Type"

  /**
   * This method does the text wrapping
   * @param text - Unformatted text
   * @return
   */
  def formatted(text: String,props: AppProperties): String = {
    WordUtils.wrap(text,props.get(Constant.EMAIL_MAX_LINE_LENGTH).toInt,"\n\r",false)
  }

}
