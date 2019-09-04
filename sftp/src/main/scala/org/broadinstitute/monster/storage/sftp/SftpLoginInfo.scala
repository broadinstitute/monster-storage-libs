package org.broadinstitute.monster.storage.sftp

/**
  * Parameters determining how to connect to an SFTP site.
  *
  * @param host hostname for the SFTP site
  * @param port port on the SFTP site to connect to
  * @param username identity to authenticate as on the SFTP site
  * @param password password for `username` in the SFTP site
  */
case class SftpLoginInfo(host: String, port: Int, username: String, password: String)
