package org.broadinstitute.monster.storage.ftp

import java.io.InputStream

import cats.effect.{Blocker, ContextShift, IO, Resource, Timer}
import cats.implicits._
import fs2.{Chunk, Stream}
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.apache.commons.net.ftp._
import org.broadinstitute.monster.storage.common.FileType

import scala.concurrent.duration._

/**
  * Concrete implementation of a client that can interact with FTP sites using Apache's commons-net library.
  *
  * @param client thin wrapper around the Apache client, for better test-ability
  * @param blocker thread pool to run all blocking operations on
  * @param readChunkSize chunk size to use when converting `InputStream`s of file contents into
  *                      fs2 streams
  * @param maxRetries max times to reopen a remote connection after the server severs the data connection
  * @param retryDelay time to sleep between attempts
  */
private[ftp] class CommonsNetFtpApi(
  client: CommonsNetFtpApi.Client,
  blocker: Blocker,
  readChunkSize: Int,
  maxRetries: Int,
  retryDelay: FiniteDuration
)(implicit cs: ContextShift[IO], t: Timer[IO])
    extends FtpApi {

  private val logger = Slf4jLogger.getLogger[IO]

  override def readFile(
    path: String,
    fromByte: Long = 0L,
    untilByte: Option[Long] = None
  ): Stream[IO, Byte] = {

    /*
     * Helper method used to stream the contents of `path` from a start position.
     *
     * If the data connection is severed by the server mid-transfer, this method
     * will recursively retry starting at the last-received byte.
     */
    def readStream(start: Long, attemptsSoFar: Int): Stream[IO, Byte] = {
      val getByteStream = for {
        _ <- logger.info(s"Opening $path at byte $start")
        bytes <- client.openRemoteFile(path, start).use {
          case None           => IO.raiseError(new RuntimeException(s"Failed to open $path"))
          case Some(inStream) => logger.info(s"Successfully opened $path").as(inStream)
        }
      } yield {
        bytes
      }

      fs2.io
        .readInputStream(getByteStream, readChunkSize, blocker)
        .chunks
        .attempt
        .scan(start -> Either.right[Throwable, Chunk[Byte]](Chunk.empty)) {
          case ((n, _), Right(bytes)) => (n + bytes.size, Right(bytes))
          case ((n, _), Left(err))    => (n, Left(err))
        }
        .flatMap {
          case (_, Right(bytes)) =>
            Stream.chunk(bytes)
          case (n, Left(err: FTPConnectionClosedException))
              if attemptsSoFar < maxRetries =>
            val message =
              s"Hit connection error while reading $path, retrying after $retryDelay"
            val logAndWait = logger.warn(err)(message).flatMap(_ => t.sleep(retryDelay))
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

  override def listDirectory(path: String): Stream[IO, (String, FileType)] = {
    val statPath =
      logger.info(s"Getting info for $path").flatMap(_ => client.statRemoteFile(path))

    Stream.eval(statPath).flatMap {
      case None =>
        Stream.raiseError[IO](
          new RuntimeException(s"Cannot list contents of nonexistent path: $path")
        )
      case Some(pathInfo) =>
        if (pathInfo.isDirectory) {
          val listDir = logger
            .info(s"Listing contents of $path")
            .flatMap(_ => client.listRemoteDirectory(path))

          Stream.eval(listDir).flatMap(Stream.emits).map { ftpFile =>
            val fileType = ftpFile.getType match {
              case FTPFile.FILE_TYPE          => FileType.File
              case FTPFile.DIRECTORY_TYPE     => FileType.Directory
              case FTPFile.SYMBOLIC_LINK_TYPE => FileType.Symlink
              case _                          => FileType.Other
            }
            (ftpFile.getName, fileType)
          }
        } else {
          Stream.raiseError[IO](
            new RuntimeException(s"Cannot list contents of non-directory: $path")
          )
        }
    }
  }
}

object CommonsNetFtpApi {

  private[ftp] val bytesPerKib = 1024
  private[ftp] val bytesPerMib = 1024 * bytesPerKib

  /** Number of bytes to buffer in each chunk of data pulled from remote files. */
  val DefaultReadChunkSize: Int = 16 * bytesPerKib

  /**
    * Number of bytes to allocate for internal buffering in the underlying FTP client.
    *
    * This value is recommended in the README.ftp files scattered around the ClinVar site.
    */
  private[ftp] val ClientBufferSize = 32 * bytesPerMib

  /** Maximum number of times to resume an in-flight transfer on data connection failures. */
  val DefaultMaxRetries: Int = 10

  /** Time to sleep between connection attempts when retrying a failed transfer. */
  val DefaultRetryDelay: FiniteDuration = 1.second

  /** Thin abstraction over commons-net's `FTPClient`, to enable mocking calls in unit tests. */
  private[ftp] trait Client {

    /**
      * Use FTP to open an input stream for a remote file, starting at an offset.
      *
      * Returns a `Resource` because file retrieval doesn't auto-terminate in FTP.
      * The constructed `InputStream` is wrapped in an `Option` because commons-net
      * returns `null` when requesting bytes for a file that doesn't exist.
      */
    def openRemoteFile(path: String, offset: Long): Resource[IO, Option[InputStream]]

    /** Use FTP to get info about a remote file, if it exists. */
    def statRemoteFile(path: String): IO[Option[FTPFile]]

    /** Use FTP to list the contents of a remote directory. */
    def listRemoteDirectory(path: String): IO[List[FTPFile]]
  }

  /**
    * Build a resource which will connect to a remote FTP site, authenticate with the site,
    * then send baseline configuration commands.
    */
  private def connectToSite(
    connectionInfo: FtpConnectionInfo,
    blocker: Blocker
  )(implicit cs: ContextShift[IO]): Resource[IO, FTPClient] = {
    val setup = blocker.blockOn {
      IO.delay {
        val client = new FTPClient()
        client.setBufferSize(ClientBufferSize)
        client.addProtocolCommandListener(FtpEventLogger)
        client.connect(connectionInfo.host, connectionInfo.port)
        client
      }.adaptError {
        case err =>
          new RuntimeException(
            s"Failed to connect to ${connectionInfo.host} on port ${connectionInfo.port}",
            err
          )
      }
    }
    val teardown = (ftp: FTPClient) => blocker.blockOn(IO.delay(ftp.disconnect()))

    Resource.make(setup)(teardown).flatMap { ftp =>
      // Connect signals failure by setting a field in the class (?!)
      val reply = ftp.getReplyCode
      val login = if (FTPReply.isPositiveCompletion(reply)) {
        blocker.blockOn {
          IO.delay {
            ftp.login(connectionInfo.username, connectionInfo.password) &&
            /*
             * VERY IMPORTANT: FTP servers distinguish requests for ASCII data
             * from requests for "binary" data. If the client asks for ASCII,
             * line endings will be re-written to the system default in-transit,
             * and transfer offsets will either be ignored or converted to a
             * character offset.
             */
            ftp.setFileType(FTP.BINARY_FILE_TYPE)
          }
        }.flatMap { success =>
          if (success) {
            IO.pure(ftp)
          } else {
            IO.raiseError(
              new RuntimeException(
                s"Failed to log into ${connectionInfo.host} as ${connectionInfo.username}"
              )
            )
          }
        }
      } else {
        IO.raiseError(
          new RuntimeException(
            s"Failed to connect to ${connectionInfo.host} on port ${connectionInfo.port}"
          )
        )
      }
      val logout = (ftp: FTPClient) => blocker.blockOn(IO.delay(ftp.logout()).void)

      Resource.make(login)(logout).evalMap { ftp =>
        val setDataMode = if (connectionInfo.passiveMode) {
          IO.delay(ftp.enterLocalPassiveMode())
        } else {
          IO.delay(ftp.enterLocalActiveMode())
        }

        setDataMode.as(ftp)
      }
    }
  }

  /**
    * Construct a client which will send authenticated requests to an FTP site.
    *
    * @param connectionInfo configuration determining which FTP site the client
    *                       will connect to
    * @param blocker execution context the client should use for all blocking I/O
    */
  def build(
    connectionInfo: FtpConnectionInfo,
    blocker: Blocker,
    readChunkSize: Int = DefaultReadChunkSize,
    maxRetries: Int = DefaultMaxRetries,
    retryDelay: FiniteDuration = DefaultRetryDelay
  )(implicit cs: ContextShift[IO], t: Timer[IO]): Resource[IO, FtpApi] = {
    val realClient = new Client {
      override def openRemoteFile(
        path: String,
        offset: Long
      ): Resource[IO, Option[InputStream]] =
        /*
         * NOTE: Apache's FTP client isn't thread-safe, so for now we default to
         * creating a new client per request. If we see a ton of overhead, we might
         * want to investigate maintaining a pool of connected clients.
         */
        connectToSite(connectionInfo, blocker).flatMap { ftp =>
          val beginCommand = blocker.blockOn {
            IO.delay {
              ftp.setRestartOffset(offset)
              ftp.retrieveFileStream(path)
            }.map(Option(_))
          }
          val finishCommand = (is: Option[InputStream]) =>
            // "complete-pending" is only valid if the byte retrieval succeeded.
            // Attempting to complete when RETR failed will cause an idle timeout.
            if (is.isDefined) {
              blocker.blockOn(IO.delay(ftp.completePendingCommand())).void
            } else {
              IO.unit
            }

          Resource.make(beginCommand)(finishCommand)
        }

      override def statRemoteFile(path: String): IO[Option[FTPFile]] =
        connectToSite(connectionInfo, blocker).use { ftp =>
          blocker.blockOn(IO.delay(ftp.mlistFile(path)))
        }.map(Option(_))

      override def listRemoteDirectory(path: String): IO[List[FTPFile]] =
        connectToSite(connectionInfo, blocker).use { ftp =>
          blocker.blockOn {
            IO.delay(ftp.listFiles(path, FTPFileFilters.NON_NULL).toList)
          }
        }
    }

    Resource.pure(
      new CommonsNetFtpApi(realClient, blocker, readChunkSize, maxRetries, retryDelay)
    )
  }
}
