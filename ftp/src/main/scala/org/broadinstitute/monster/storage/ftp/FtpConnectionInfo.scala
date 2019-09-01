package org.broadinstitute.monster.storage.ftp

/**
  * Parameters determining how to connect to an FTP site.
  *
  * @param host hostname for the FTP site
  * @param port port on the FTP site to connect to
  * @param passiveMode if true, the client will connect in FTP passive mode.
  *                    Otherwise it will use FTP active mode.
  * @param username identity to authenticate as on the FTP site. According to
  *                 the FTP spec, set to 'anonymous' for unauthenticated access
  * @param password password for `username` in the FTP site, or a valid email
  *                 if connecting anonymously
  *
  * @see https://tools.ietf.org/html/rfc1635 for more info on configuring
  *      anonymous access
  */
case class FtpConnectionInfo(
  host: String,
  port: Int,
  passiveMode: Boolean,
  username: String,
  password: String
)
