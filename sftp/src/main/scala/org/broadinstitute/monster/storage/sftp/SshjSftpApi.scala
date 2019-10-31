package org.broadinstitute.monster.storage.sftp

import java.io.InputStream

import cats.effect.concurrent.Ref
import cats.effect.{Blocker, ContextShift, IO, Resource, Timer}
import cats.implicits._
import fs2.{Chunk, Stream}
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import net.schmizz.keepalive.KeepAliveProvider
import net.schmizz.sshj.{DefaultConfig, SSHClient}
import net.schmizz.sshj.common.SSHException
import net.schmizz.sshj.sftp.{
  FileMode,
  RemoteResourceInfo,
  SFTPClient,
  FileAttributes => RawAttributes
}
import net.schmizz.sshj.transport.verification.PromiscuousVerifier
import org.broadinstitute.monster.storage.common.{FileAttributes, FileType}
import retry._
import retry.CatsEffect._

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
  * @param maxRetryDelay max time to sleep between attempts
  */
private[sftp] class SshjSftpApi(
  client: SshjSftpApi.Client,
  blocker: Blocker,
  readChunkSize: Int,
  maxRetries: Int,
  maxRetryDelay: FiniteDuration
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
        fs2.io
          .readInputStream(client.openRemoteFile(path, start), readChunkSize, blocker)
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
              val delay = maxRetryDelay.min(math.pow(2.0, attemptsSoFar.toDouble).seconds)
              val message =
                s"Hit connection error while reading $path, retrying after $delay"
              val logAndWait = for {
                _ <- logger.warn(err)(message)
                _ <- client.reset()
                _ <- t.sleep(delay)
              } yield ()

              Stream.eval(logAndWait) >> readStream(n, attemptsSoFar + 1)
            case (_, Left(err)) =>
              Stream.raiseError[IO](err)
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

  private val logger = Slf4jLogger.getLogger[IO]

  private[sftp] val bytesPerKib = 1024

  /** Number of bytes to buffer in each chunk of data pulled from remote files. */
  val DefaultReadChunkSize: Int = 16 * bytesPerKib

  /** Maximum number of times to resume an in-flight transfer on data connection failures. */
  val DefaultMaxRetries: Int = 10

  /** Time to sleep between connection attempts when retrying a failed transfer. */
  val DefaultRetryDelay: FiniteDuration = 1.second

  /** Number of read requests to keep in-flight at a time for a single transfer stream. */
  val DefaultReadAhead: Int = 16

  /** Thin abstraction over sshj's `SFTPClient`, to enable mocking calls in unit tests. */
  private[sftp] trait Client {

    /** Use SFTP to open an input stream for a remote file, starting at an offset. */
    def openRemoteFile(path: String, offset: Long): IO[InputStream]

    /** Use SFTP to check if a remote file exists, returning its info if so. */
    def statRemoteFile(path: String): IO[Option[RawAttributes]]

    /** Use SFTP to list the contents of a remote directory. */
    def listRemoteDirectory(path: String): IO[List[RemoteResourceInfo]]

    /** Reset (close and re-create) the machinery backing this client. */
    def reset(): IO[Unit]

    /** Close the machinery backing this client. */
    def close(): IO[Unit]
  }

  /**
    * Connect to a remote host over SSH and authenticate with the site,
    * returning the SSH client and a linked SFTP client.
    */
  private def connectToHost(
    loginInfo: SftpLoginInfo,
    blocker: Blocker,
    maxPacketSize: Int
  )(
    implicit cs: ContextShift[IO]
  ): IO[(SSHClient, SFTPClient)] = blocker.blockOn {
    for {
      ssh <- IO.delay {
        val config = new DefaultConfig()
        config.setKeepAliveProvider(KeepAliveProvider.KEEP_ALIVE)

        val ssh = new SSHClient(config)
        ssh.addHostKeyVerifier(new PromiscuousVerifier())
        ssh.getConnection.setMaxPacketSize(maxPacketSize)
        ssh
      }
      _ <- IO.delay(ssh.connect(loginInfo.host, loginInfo.port)).adaptError {
        case err =>
          new RuntimeException(
            s"Failed to connect to ${loginInfo.host} on port ${loginInfo.port}",
            err
          )
      }
      _ <- IO.delay(ssh.authPassword(loginInfo.username, loginInfo.password)).adaptError {
        case err =>
          new RuntimeException(
            s"Failed to log into ${loginInfo.host} as ${loginInfo.username}",
            err
          )
      }
      sftp <- IO.delay(ssh.newSFTPClient())
    } yield {
      (ssh, sftp)
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
    maxRetryDelay: FiniteDuration = DefaultRetryDelay,
    readAhead: Int = DefaultReadAhead
  )(
    implicit cs: ContextShift[IO],
    t: Timer[IO]
  ): Resource[IO, SftpApi] = {
    val connectionRetryPolicy =
      RetryPolicies.limitRetries[IO](maxRetries).join {
        RetryPolicies
          .capDelay(maxRetryDelay, RetryPolicies.exponentialBackoff(1.second))
      }

    // Error handler for initial SSH connection attempts.
    // Nothing meaningful to do here but log.
    def handleConnectionError(err: Throwable, details: RetryDetails): IO[Unit] =
      details match {
        case RetryDetails.WillDelayAndRetry(nextDelay, retriesSoFar, cumulativeDelay) =>
          logger.warn(err)(
            s"Failed to make SFTP connection after $retriesSoFar attempts ($cumulativeDelay), will try again in $nextDelay"
          )
        case RetryDetails.GivingUp(totalRetries, totalDelay) =>
          logger.error(err)(
            s"Failed to make SFTP connection after $totalRetries attempts ($totalDelay)"
          )
      }

    // Utility to set up a new SSH/SFTP connection, with baked-in retry logic & logging.
    def connect(): IO[(SSHClient, SFTPClient)] =
      retryingOnAllErrors(
        policy = connectionRetryPolicy,
        onError = handleConnectionError
      )(connectToHost(loginInfo, blocker, readChunkSize))

    val setup = connect().flatMap {
      case (initSsh, initSftp) =>
        /*
         * For efficiency, we want to keep our SSH / SFTP clients open for as long as possible.
         * Since there's always a chance that some error will cause a long-lived connection to
         * be terminated (and the SSH library we're using doesn't support client reuse after disconnect),
         * we have to support swapping out the "active" clients at runtime.
         *
         * Begin by initializing a ~mutable Ref containing the first clients-to-use.
         */
        Ref[IO].of((initSsh, initSftp)).map { clientsRef =>
          // Utility to tear down the clients currently held by our reference.
          def closeSftp(): IO[Unit] = clientsRef.get.flatMap {
            case (ssh, sftp) =>
              blocker.blockOn {
                IO.delay {
                  sftp.close()
                  ssh.close()
                }
              }
          }

          // Utility to replace the cached clients held by our reference.
          def resetSftp(): IO[SFTPClient] =
            for {
              _ <- closeSftp()
              (newSsh, newSftp) <- connect()
              _ <- clientsRef.set((newSsh, newSftp))
            } yield {
              newSftp
            }

          // Run a function using a connected SFTP client.
          // If the currently-cached client has been disconnected, this method
          // will replace it with a new client before running the provided function.
          def useSftp[A](f: SFTPClient => IO[A]): IO[A] =
            for {
              (ssh, sftp) <- clientsRef.get
              sftpToUse <- if (ssh.isConnected && ssh.isAuthenticated) {
                IO.pure(sftp)
              } else {
                resetSftp()
              }
              out <- f(sftpToUse)
            } yield {
              out
            }

          /*
           * NOTE: sshj's client is thread-safe, but long-lived SSH sessions are still
           * liable to be terminated at any time, so to make our lives easy we create
           * a new client / connection per request. If we see a ton of overhead, we might
           * want to investigate maintaining a pool of connected clients.
           */
          new Client {
            override def openRemoteFile(path: String, offset: Long): IO[InputStream] =
              useSftp { sftp =>
                blocker.blockOn(IO.delay(sftp.open(path))).map { file =>
                  new file.ReadAheadRemoteFileInputStream(readAhead, offset)
                }
              }

            override def statRemoteFile(path: String): IO[Option[RawAttributes]] =
              useSftp(sftp => IO.delay(sftp.statExistence(path)).map(Option.apply))

            override def listRemoteDirectory(path: String): IO[List[RemoteResourceInfo]] =
              useSftp(sftp => IO.delay(sftp.ls(path)).map(_.asScala.toList))

            override def reset(): IO[Unit] = resetSftp().void

            override def close(): IO[Unit] = closeSftp()
          }
        }
    }

    Resource
      .make(setup)(_.close())
      .map(new SshjSftpApi(_, blocker, readChunkSize, maxRetries, maxRetryDelay))
  }
}
