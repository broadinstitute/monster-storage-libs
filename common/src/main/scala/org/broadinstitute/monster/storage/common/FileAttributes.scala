package org.broadinstitute.monster.storage.common

/**
  * Minimal attributes that are useful to know about a remote file.
  *
  * @param size number of bytes in the file
  * @param md5 MD5-hash of the file's contents, if known
  */
case class FileAttributes(size: Long, md5: Option[String])
