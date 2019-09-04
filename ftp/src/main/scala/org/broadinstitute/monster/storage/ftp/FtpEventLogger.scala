package org.broadinstitute.monster.storage.ftp

import cats.effect.IO
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.apache.commons.net.{ProtocolCommandEvent, ProtocolCommandListener}

/** Logging adapter for commons-net's FTP event-listener infrastructure. */
object FtpEventLogger extends ProtocolCommandListener {

  private val logger = Slf4jLogger.getLogger[IO]

  override def protocolCommandSent(event: ProtocolCommandEvent): Unit = {
    val cmd = event.getCommand

    // Make sure we don't log sensitive info
    val sanitized = if ("PASS".equalsIgnoreCase(cmd)) {
      "PASS ******"
    } else {
      event.getMessage
    }

    logger
      .debug(s"Sent FTP command with message: $sanitized")
      .unsafeRunAsyncAndForget()
  }

  override def protocolReplyReceived(event: ProtocolCommandEvent): Unit =
    logger
      .debug(s"Received FTP reply with message: ${event.getMessage}")
      .unsafeRunAsyncAndForget()
}
