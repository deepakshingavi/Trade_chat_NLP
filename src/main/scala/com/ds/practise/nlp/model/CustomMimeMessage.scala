package com.ds.practise.nlp.model

import javax.mail.internet.MimeMessage
import javax.mail.{MessagingException, Session}

class CustomMimeMessage(session: Session, messageId: String, mimeVersion: String, contentTransferEncoding: String, contentType: String) extends MimeMessage(session) {


  @throws[MessagingException]
  override protected def updateMessageID(): Unit = {
    setHeader("Message-ID", messageId)
    setHeader("MIME-Version", mimeVersion)
    setHeader("Content-Transfer-Encoding", contentTransferEncoding)
    setHeader("Content-Type", contentType)
  }

}

