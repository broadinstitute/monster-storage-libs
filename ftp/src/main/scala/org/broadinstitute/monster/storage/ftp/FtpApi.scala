package org.broadinstitute.monster.storage.ftp

import cats.effect.IO
import fs2.Stream
import org.broadinstitute.monster.storage.common.FileType

/**
  * Client which can perform I/O operations against an FTP site.
  *
  * NOTE: Unlike our clients for public-cloud storage, an instance of
  * this client can only be used against a single FTP site.
  */
trait FtpApi {

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
  ): Stream[IO, Byte]

  /**
    * List the contents of a remote directory, returning their names and file types.
    *
    * @param path path within the configured FTP site pointing to the directory-to-list
    */
  def listDirectory(path: String): Stream[IO, (String, FileType)]
}
