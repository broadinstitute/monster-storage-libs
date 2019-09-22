package org.broadinstitute.monster.storage.sftp

import java.io.InputStream

import cats.effect.{Blocker, ContextShift, IO, Resource, Timer}
import cats.implicits._
import fs2.{Chunk, Stream}
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import net.schmizz.sshj.SSHClient
import net.schmizz.sshj.common.SSHException
import net.schmizz.sshj.sftp.{
  FileMode,
  RemoteResourceInfo,
  SFTPClient,
  FileAttributes => RawAttributes
}
import net.schmizz.sshj.transport.verification.PromiscuousVerifier
import org.broadinstitute.monster.storage.common.{FileAttributes, FileType}

import scala.collection.JavaConverters._
import scala.concurrent.duration._

/**
  * Concrete implementation of a client that can interact with SFT sites using the sshj library.
  *
  * @param client thin wrapper around the sshj client, for better test-ability
  * @param blocker thread pool to run all blocking operations on
  * @param readChunkSize chunk size to use when converting `InputStream`s of file contents into
  *                      fs2 streams
  * @param maxRetries max times to reopen a remote connection after the server severs the data
  *                   connection
  * @param retryDelay time to sleep between attempts
  */
private[sftp] class SshjSftpApi(
  client: SshjSftpApi.Client,
  blocker: Blocker,
  readChunkSize: Int,
  maxRetries: Int,
  retryDelay: FiniteDuration
)(implicit cs: ContextShift[IO], t: Timer[IO])
    extends SftpApi {

  private val logger = Slf4jLogger.getLogger[IO]

  override def readFile(
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
    def readStream(start: Long, attemptsSoFar: Int): Stream[IO, Byte] =
      Stream.eval(logger.info(s"Opening $path at byte $start")) >>
        Stream.resource(client.openRemoteFile(path, start)).flatMap { inStream =>
          fs2.io
            .readInputStream(IO.pure(inStream), readChunkSize, blocker)
            .chunks
            .attempt
            .scan(start -> Either.right[Throwable, Chunk[Byte]](Chunk.empty)) {
              case ((n, _), Right(bytes)) => (n + bytes.size, Right(bytes))
              case ((n, _), Left(err))    => (n, Left(err))
            }
            .flatMap {
              case (_, Right(bytes)) =>
                Stream.chunk(bytes)
              case (n, Left(err: SSHException)) if attemptsSoFar < maxRetries =>
                val message =
                  s"Hit connection error while reading $path, retrying after $retryDelay"
                val logAndWait =
                  logger.warn(err)(message).flatMap(_ => t.sleep(retryDelay))
                Stream.eval(logAndWait) >> readStream(n, attemptsSoFar + 1)
              case (_, Left(err)) =>
                Stream.raiseError[IO](err)
            }
        }

    val allBytes = readStream(fromByte, 0)
    untilByte
      .fold(allBytes)(lastByte => allBytes.take(lastByte - fromByte))
      .handleErrorWith { err =>
        Stream
          .raiseError[IO](new RuntimeException(s"Failed to read bytes from $path", err))
      }
  }

  override def statFile(path: String): IO[Option[FileAttributes]] =
    blocker
      .blockOn(client.statRemoteFile(path))
      .map(_.map(attrs => FileAttributes(attrs.getSize, None)))

  override def listDirectory(path: String): Stream[IO, (String, FileType)] = {
    val getEntries = blocker.blockOn(client.listRemoteDirectory(path))

    Stream
      .eval(getEntries)
      .flatMap(Stream.emits)
      .map { info =>
        val fileType = info.getAttributes.getType match {
          case FileMode.Type.REGULAR   => FileType.File
          case FileMode.Type.DIRECTORY => FileType.Directory
          case FileMode.Type.SYMLINK   => FileType.Symlink
          case _                       => FileType.Other
        }
        (info.getPath, fileType)
      }
      .handleErrorWith { err =>
        Stream.raiseError[IO](new RuntimeException(s"Failed to list $path", err))
      }
  }
}

object SshjSftpApi {

  private[sftp] val bytesPerKib = 1024

  /** Number of bytes to buffer in each chunk of data pulled from remote files. */
  val DefaultReadChunkSize: Int = 16 * bytesPerKib

  /** Maximum number of times to resume an in-flight transfer on data connection failures. */
  val DefaultMaxRetries: Int = 10

  /** Time to sleep between connection attempts when retrying a failed transfer. */
  val DefaultRetryDelay: FiniteDuration = 1.second

  /** Thin abstraction over sshj's `SFTPClient`, to enable mocking calls in unit tests. */
  private[sftp] trait Client {

    /** Use SFTP to open an input stream for a remote file, starting at an offset. */
    def openRemoteFile(path: String, offset: Long): Resource[IO, InputStream]

    /** Use SFTP to check if a remote file exists, returning its info if so. */
    def statRemoteFile(path: String): IO[Option[RawAttributes]]

    /** Use SFTP to list the contents of a remote directory. */
    def listRemoteDirectory(path: String): IO[List[RemoteResourceInfo]]
  }

  /**
    * Build a resource which will connect to a remote host over SSH, authenticate
    * with the site, then create an SFTP client for the host.
    */
  private def connectToHost(loginInfo: SftpLoginInfo, blocker: Blocker)(
    implicit cs: ContextShift[IO]
  ): Resource[IO, SFTPClient] = {
    val sshSetup = blocker
      .blockOn(IO.delay(new SSHClient()))
      .flatTap(ssh => IO.delay(ssh.addHostKeyVerifier(new PromiscuousVerifier())))
    val sshTeardown = (ssh: SSHClient) => blocker.blockOn(IO.delay(ssh.close()))

    Resource.make(sshSetup)(sshTeardown).flatMap { ssh =>
      val sshConnect = blocker.blockOn {
        for {
          _ <- IO.delay(ssh.connect(loginInfo.host, loginInfo.port)).adaptError {
            case err =>
              new RuntimeException(
                s"Failed to connect to ${loginInfo.host} on port ${loginInfo.port}",
                err
              )
          }
          _ <- IO
            .delay(ssh.authPassword(loginInfo.username, loginInfo.password))
            .adaptError {
              case err =>
                new RuntimeException(
                  s"Failed to log into ${loginInfo.host} as ${loginInfo.username}",
                  err
                )
            }
        } yield {
          ssh
        }
      }
      val sshDisconnect =
        (ssh: SSHClient) => blocker.blockOn(IO.delay(ssh.disconnect()))

      Resource.make(sshConnect)(sshDisconnect).flatMap { connectedSsh =>
        val sftpSetup = blocker.blockOn(IO.delay(connectedSsh.newSFTPClient()))
        val sftpTeardown =
          (sftp: SFTPClient) => blocker.blockOn(IO.delay(sftp.close()))

        Resource.make(sftpSetup)(sftpTeardown)
      }
    }
  }

  /**
    * Construct a client which will send authenticated requests to an SFTP site.
    *
    * @param loginInfo configuration determining which SFTP site the client will
    *                  connect to
    * @param blocker execution context the client should use for all blocking I/O
    */
  def build(
    loginInfo: SftpLoginInfo,
    blocker: Blocker,
    readChunkSize: Int = DefaultReadChunkSize,
    maxRetries: Int = DefaultMaxRetries,
    retryDelay: FiniteDuration = DefaultRetryDelay
  )(
    implicit cs: ContextShift[IO],
    t: Timer[IO]
  ): Resource[IO, SftpApi] = {
    /*
     * NOTE: sshj's client is thread-safe, but long-lived SSH sessions are still
     * liable to be terminated at any time, so to make our lives easy we create
     * a new client / connection per request. If we see a ton of overhead, we might
     * want to investigate maintaining a pool of connected clients.
     */
    val realClient = new Client {
      override def openRemoteFile(path: String, offset: Long): Resource[IO, InputStream] =
        connectToHost(loginInfo, blocker).evalMap { sftp =>
          blocker.blockOn(IO.delay(sftp.open(path))).map { file =>
            new file.RemoteFileInputStream(offset)
          }
        }

      override def statRemoteFile(path: String): IO[Option[RawAttributes]] =
        connectToHost(loginInfo, blocker).use { sftp =>
          IO.delay(sftp.statExistence(path)).map(Option.apply)
        }

      override def listRemoteDirectory(path: String): IO[List[RemoteResourceInfo]] =
        connectToHost(loginInfo, blocker).use { sftp =>
          IO.delay(sftp.ls(path)).map(_.asScala.toList)
        }
    }

    Resource.pure(
      new SshjSftpApi(realClient, blocker, readChunkSize, maxRetries, retryDelay)
    )
  }
}
