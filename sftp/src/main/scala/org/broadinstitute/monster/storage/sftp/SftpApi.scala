package org.broadinstitute.monster.storage.sftp

import cats.effect.IO
import fs2.Stream
import org.broadinstitute.monster.storage.common.{FileAttributes, FileType}

/**
  * Client which can perform I/O operations against an SFTP site.
  *
  * NOTE: Unlike our clients for public-cloud storage, an instance of
  * this client can only be used against a single SFTP site.
  */
trait SftpApi {

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
  ): Stream[IO, Byte]

  /**
    * Check if an SFTP file exists, returning useful attributes about it if so.
    *
    * @param path path within the configured SFTP site pointing to the potential file.
    */
  def statFile(path: String): IO[Option[FileAttributes]]

  /**
    * List the contents of an SFTP directory.
    *
    * @param path within the configured SFTP site pointing to the directory to list
    * @return name/type pairs for all entries in the listed directory
    */
  def listDirectory(path: String): Stream[IO, (String, FileType)]
}
