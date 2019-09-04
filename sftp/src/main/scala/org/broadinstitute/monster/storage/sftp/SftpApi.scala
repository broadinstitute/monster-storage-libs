package org.broadinstitute.monster.storage.sftp

import java.io.InputStream

import cats.effect.{ContextShift, IO, Resource}
import cats.implicits._
import fs2.{Chunk, Stream}
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import net.schmizz.sshj.SSHClient
import net.schmizz.sshj.common.SSHException
import net.schmizz.sshj.sftp.{FileMode, RemoteResourceInfo, SFTPClient}
import net.schmizz.sshj.transport.verification.PromiscuousVerifier

import scala.concurrent.ExecutionContext
import scala.collection.JavaConverters._

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

  private val logger = Slf4jLogger.getLogger[IO]

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

    /*
     * Helper method used to stream the contents of `path` from a start position.
     *
     * If the SSH connection is severed mid-transfer, this method will recursively
     * retry starting at the last-received byte.
     */
    def readStream(start: Long): Stream[IO, Byte] =
      Stream.eval(logger.info(s"Opening $path at byte $start")) >>
        Stream.resource(client.openRemoteFile(path, start)).flatMap { inStream =>
          fs2.io
            .readInputStream(IO.pure(inStream), ReadChunkSize, blockingEc)
            .chunks
            .attempt
            .scan(start -> Either.right[Throwable, Chunk[Byte]](Chunk.empty)) {
              case ((n, _), Right(bytes)) => (n + bytes.size, Right(bytes))
              case ((n, _), Left(err))    => (n, Left(err))
            }
            .flatMap {
              case (_, Right(bytes)) =>
                Stream.chunk(bytes)
              case (n, Left(err: SSHException)) =>
                val message = s"Hit connection error while reading $path, retrying"
                Stream.eval(logger.warn(err)(message)) >> readStream(n)
              case (_, Left(err)) =>
                Stream.raiseError[IO](err)
            }
        }

    val allBytes = readStream(fromByte)
    untilByte
      .fold(allBytes)(lastByte => allBytes.take(lastByte - fromByte))
      .handleErrorWith { err =>
        Stream
          .raiseError[IO](new RuntimeException(s"Failed to read bytes from $path", err))
      }
  }

  /**
    * List the contents of an SFTP directory.
    *
    * @param path within the configured SFTP site pointing to the directory to list
    * @return name/type pairs for all entries in the listed directory
    */
  def listDirectory(path: String): Stream[IO, (String, FileMode.Type)] = {
    val getEntries = cs.evalOn(blockingEc)(client.listRemoteDirectory(path))

    Stream
      .eval(getEntries)
      .flatMap(Stream.emits)
      .map { info =>
        (info.getPath, info.getAttributes.getType)
      }
      .handleErrorWith { err =>
        Stream.raiseError[IO](new RuntimeException(s"Failed to list $path", err))
      }
  }
}

object SftpApi {

  /** Number of bytes to buffer in each chunk of data pulled from remote files. */
  val ReadChunkSize = 8192

  /** Thin abstraction over sshj's `SFTPClient`, to enable mocking calls in unit tests. */
  private[sftp] trait Client {

    /** Use SFTP to open an input stream for a remote file, starting at an offset. */
    def openRemoteFile(path: String, offset: Long): Resource[IO, InputStream]

    /** Use SFTP to list the contents of a remote directory. */
    def listRemoteDirectory(path: String): IO[List[RemoteResourceInfo]]
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
    * Construct a client which will send authenticated requests to an SFTP site.
    *
    * @param loginInfo configuration determining which SFTP site the client will
    *                  connect to
    * @param blockingEc execution context the client should use for all blocking I/O
    */
  def build(loginInfo: SftpLoginInfo, blockingEc: ExecutionContext)(
    implicit cs: ContextShift[IO]
  ): Resource[IO, SftpApi] = {
    /*
     * NOTE: sshj's client is thread-safe, but long-lived SSH sessions are still
     * liable to be terminated at any time, so to make our lives easy we create
     * a new client / connection per request. If we see a ton of overhead, we might
     * want to investigate maintaining a pool of connected clients.
     */
    val realClient = new Client {
      override def openRemoteFile(path: String, offset: Long): Resource[IO, InputStream] =
        connectToHost(loginInfo, blockingEc).evalMap { sftp =>
          cs.evalOn(blockingEc)(IO.delay(sftp.open(path))).map { file =>
            new file.RemoteFileInputStream(offset)
          }
        }

      override def listRemoteDirectory(path: String): IO[List[RemoteResourceInfo]] =
        connectToHost(loginInfo, blockingEc).use { sftp =>
          IO.delay(sftp.ls(path)).map(_.asScala.toList)
        }
    }

    Resource.pure(new SftpApi(realClient, blockingEc))
  }
}
