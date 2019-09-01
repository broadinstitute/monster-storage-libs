package org.broadinstitute.monster.storage.sftp

import java.io.InputStream

import cats.effect.{ContextShift, IO, Resource}
import cats.implicits._
import fs2.Stream
import net.schmizz.sshj.SSHClient
import net.schmizz.sshj.sftp.SFTPClient
import net.schmizz.sshj.transport.verification.PromiscuousVerifier

import scala.concurrent.ExecutionContext

/**
  * Client which can perform I/O operations against an SFTP site.
  *
  * NOTE: Unlike our clients for public-cloud storage, an instance of
  * this client can only be used against a single SFTP site.
  */
class SftpApi private[sftp] (
  client: SftpApi.Client,
  blockingEc: ExecutionContext
)(implicit cs: ContextShift[IO]) {
  import SftpApi._

  /**
    * Read a range of bytes (potentially the whole file) from an SFTP file.
    *
    * @param path path within the configured SFTP site pointing to the file-to-read
    * @param fromByte first byte (zero-indexed, inclusive) within the file at `path`
    *                 which should be included in the response
    * @param untilByte exclusive endpoint for the bytes returned from the file at `path`,
    *                  or `None` if all bytes should be returned
    */
  def readFile(
    path: String,
    fromByte: Long = 0L,
    untilByte: Option[Long] = None
  ): Stream[IO, Byte] = {
    val getByteStream = cs.evalOn(blockingEc)(client.openRemoteFile(path, fromByte))
    val allBytes = fs2.io.readInputStream(getByteStream, ReadChunkSize, blockingEc)

    untilByte
      .fold(allBytes)(lastByte => allBytes.take(lastByte - fromByte))
      .handleErrorWith { err =>
        Stream
          .raiseError[IO](new RuntimeException(s"Failed to read bytes from $path", err))
      }
  }
}

object SftpApi {

  /** Number of bytes to buffer in each chunk of data pulled from remote files. */
  val ReadChunkSize = 8192

  /** Thin abstraction over sshj's `SFTPClient`, to enable mocking calls in unit tests. */
  trait Client {

    /** Use SFTP to open an input stream for a remote file, starting at an offset. */
    def openRemoteFile(path: String, offset: Long): IO[InputStream]
  }

  /**
    * Build a resource which will connect to a remote host over SSH, authenticate
    * with the site, then create an SFTP client for the host.
    */
  private def connectToHost(loginInfo: SftpLoginInfo, blockingEc: ExecutionContext)(
    implicit cs: ContextShift[IO]
  ): Resource[IO, SFTPClient] = {
    val sshSetup = cs
      .evalOn(blockingEc)(IO.delay(new SSHClient()))
      .flatTap(ssh => IO.delay(ssh.addHostKeyVerifier(new PromiscuousVerifier())))
    val sshTeardown = (ssh: SSHClient) => cs.evalOn(blockingEc)(IO.delay(ssh.close()))

    Resource.make(sshSetup)(sshTeardown).flatMap { ssh =>
      val sshConnect = cs.evalOn(blockingEc) {
        for {
          _ <- IO.delay(ssh.connect(loginInfo.host, loginInfo.port))
          _ <- IO.delay(ssh.authPassword(loginInfo.username, loginInfo.password))
        } yield {
          ssh
        }
      }
      val sshDisconnect =
        (ssh: SSHClient) => cs.evalOn(blockingEc)(IO.delay(ssh.disconnect()))

      Resource.make(sshConnect)(sshDisconnect).flatMap { connectedSsh =>
        val sftpSetup = cs.evalOn(blockingEc)(IO.delay(connectedSsh.newSFTPClient()))
        val sftpTeardown =
          (sftp: SFTPClient) => cs.evalOn(blockingEc)(IO.delay(sftp.close()))

        Resource.make(sftpSetup)(sftpTeardown)
      }
    }
  }

  /**
    * Construct a client which will send authorized requests to an SFTP site.
    *
    * @param loginInfo configuration determining which SFTP site the client will
    *                  connect to
    * @param blockingEc execution context the client should use for all blocking I/O
    */
  def build(loginInfo: SftpLoginInfo, blockingEc: ExecutionContext)(
    implicit cs: ContextShift[IO]
  ): Resource[IO, SftpApi] =
    connectToHost(loginInfo, blockingEc).map { sftp =>
      val realClient = new Client {
        override def openRemoteFile(path: String, offset: Long): IO[InputStream] =
          for {
            file <- IO.delay(sftp.open(path))
            inStream <- IO.delay(new file.RemoteFileInputStream(offset))
          } yield {
            inStream: InputStream
          }
      }

      new SftpApi(realClient, blockingEc)
    }
}
