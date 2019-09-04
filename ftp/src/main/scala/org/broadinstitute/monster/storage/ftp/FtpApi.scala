package org.broadinstitute.monster.storage.ftp

import java.io.InputStream

import cats.effect.{ContextShift, IO, Resource}
import cats.implicits._
import fs2.{Chunk, Stream}
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.apache.commons.net.ftp._

import scala.concurrent.ExecutionContext

/**
  * Client which can perform I/O operations against an FTP site.
  *
  * NOTE: Unlike our clients for public-cloud storage, an instance of
  * this client can only be used against a single FTP site.
  */
class FtpApi private[ftp] (
  client: FtpApi.Client,
  blockingEc: ExecutionContext
)(implicit cs: ContextShift[IO]) {
  import FtpApi._

  private val logger = Slf4jLogger.getLogger[IO]

  /**
    * Read a range of bytes (potentially the whole file) from an FTP site.
    *
    * @param path path within the configured FTP site pointing to the file-to-read
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
     * If the data connection is severed by the server mid-transfer, this method
     * will recursively retry starting at the last-received byte.
     */
    def readStream(start: Long): Stream[IO, Byte] = {
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
        .readInputStream(getByteStream, ReadChunkSize, blockingEc)
        .chunks
        .attempt
        .scan(start -> Either.right[Throwable, Chunk[Byte]](Chunk.empty)) {
          case ((n, _), Right(bytes)) => (n + bytes.size, Right(bytes))
          case ((n, _), Left(err))    => (n, Left(err))
        }
        .flatMap {
          case (_, Right(bytes)) =>
            Stream.chunk(bytes)
          case (n, Left(err: FTPConnectionClosedException)) =>
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
    * List the contents of a remote directory, returning their names and file types.
    *
    * @param path path within the configured FTP site pointing to the directory-to-list
    */
  def listDirectory(path: String): Stream[IO, (String, FileType)] = {
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
            val tpe = ftpFile.getType match {
              case FTPFile.FILE_TYPE          => RegularFile
              case FTPFile.DIRECTORY_TYPE     => Directory
              case FTPFile.SYMBOLIC_LINK_TYPE => Symlink
              case _                          => Other
            }
            (ftpFile.getName, tpe)
          }
        } else {
          Stream.raiseError[IO](
            new RuntimeException(s"Cannot list contents of non-directory: $path")
          )
        }
    }
  }
}

object FtpApi {

  private[ftp] val bytesPerKib = 1024
  private[ftp] val bytesPerMib = 1024 * bytesPerKib

  /** Number of bytes to buffer in each chunk of data pulled from remote files. */
  val ReadChunkSize = 8 * bytesPerKib

  /**
    * Number of bytes to allocate for internal buffering in the underlying FTP client.
    *
    * This value is recommended in the README.ftp files scattered around the ClinVar site.
    */
  val ClientBufferSize = 32 * bytesPerMib

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
    * Enum representation of FTP file types.
    *
    * TODO: This representation isn't bound to FTP. It'd probably be worth breaking
    * out a new 'common' project to hold its definition, and then have each lib adapt
    * their specific methods to use it.
    */
  sealed trait FileType
  case object RegularFile extends FileType
  case object Directory extends FileType
  case object Symlink extends FileType
  case object Other extends FileType

  /**
    * Build a resource which will connect to a remote FTP site, authenticate with the site,
    * then send baseline configuration commands.
    */
  private def connectToSite(
    connectionInfo: FtpConnectionInfo,
    blockingEc: ExecutionContext
  )(implicit cs: ContextShift[IO]): Resource[IO, FTPClient] = {
    val setup = cs.evalOn(blockingEc) {
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
    val teardown = (ftp: FTPClient) => cs.evalOn(blockingEc)(IO.delay(ftp.disconnect()))

    Resource.make(setup)(teardown).flatMap { ftp =>
      // Connect signals failure by setting a field in the class (?!)
      val reply = ftp.getReplyCode
      val login = if (FTPReply.isPositiveCompletion(reply)) {
        cs.evalOn(blockingEc) {
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
          }
          .flatMap { success =>
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
      val logout = (ftp: FTPClient) => cs.evalOn(blockingEc)(IO.delay(ftp.logout()).void)

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
    * @param blockingEc execution context the client should use for all blocking I/O
    */
  def build(
    connectionInfo: FtpConnectionInfo,
    blockingEc: ExecutionContext
  )(implicit cs: ContextShift[IO]): Resource[IO, FtpApi] = {
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
        connectToSite(connectionInfo, blockingEc).flatMap { ftp =>
          val beginCommand = cs.evalOn(blockingEc) {
            IO.delay {
              ftp.setRestartOffset(offset)
              ftp.retrieveFileStream(path)
            }.map(Option(_))
          }
          val finishCommand = (is: Option[InputStream]) =>
            // "complete-pending" is only valid if the byte retrieval succeeded.
            // Attempting to complete when RETR failed will cause an idle timeout.
            if (is.isDefined) {
              cs.evalOn(blockingEc)(IO.delay(ftp.completePendingCommand())).void
            } else {
              IO.unit
            }

          Resource.make(beginCommand)(finishCommand)
        }

      override def statRemoteFile(path: String): IO[Option[FTPFile]] =
        connectToSite(connectionInfo, blockingEc).use { ftp =>
          cs.evalOn(blockingEc) {
            IO.delay(ftp.mlistFile(path))
          }
        }.map(Option(_))

      override def listRemoteDirectory(path: String): IO[List[FTPFile]] =
        connectToSite(connectionInfo, blockingEc).use { ftp =>
          cs.evalOn(blockingEc) {
            IO.delay(ftp.listFiles(path, FTPFileFilters.NON_NULL).toList)
          }
        }
    }

    Resource.pure(new FtpApi(realClient, blockingEc))
  }
}
