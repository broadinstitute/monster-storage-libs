package org.broadinstitute.monster.storage.ftp

import cats.effect.{ContextShift, IO, Timer}
import fs2.Stream
import org.broadinstitute.monster.storage.common.FileType
import org.scalatest.{EitherValues, FlatSpec, Matchers}

import scala.concurrent.ExecutionContext

class FtpApiIntegrationSpec extends FlatSpec with Matchers with EitherValues {

  private implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  private implicit val t: Timer[IO] = IO.timer(ExecutionContext.global)

  // FIXME: Change this to point to our own FTP server once we have one up and running.
  // NCBI's site is maddeningly slow, you probably want to take a break once you launch these tests...
  private val testInfo = FtpConnectionInfo(
    "ftp.ncbi.nlm.nih.gov",
    21,
    passiveMode = true,
    /*
     * The FTP spec allows servers to configure anonymous access by permitting
     * a magic 'anonymous' user. The user doesn't really have a password, but
     * it's considered good practice to use an email address so site admins can
     * see who's accessing their data.
     *
     * See: https://tools.ietf.org/html/rfc1635
     */
    "anonymous",
    "monster@broadinstitute.org"
  )
  private val testPath = "pub/README.ftp"
  private val testContent =
    s"""
       |Hello!
       |
       |NCBI ftp server supports FTP protocol. It does not support SFTP protocol.
       |The FTP client should run in passive FTP mode.
       |
       |After series of experiments with ftp clients tuning for both uploading
       |and downloading with NCBI's ftp server we got to the conclusion that the
       |configuration of ftp client seriously affects the speed you get.
       |
       |We recommend to increase the buffer size to 32MB, it helps for both
       |uploads and downloads.
       |
       |In ncftp command line utility the buffer size can be changed via "set
       |so-bufsize" command.
       |
       |Example, download from st-va colo to be-md:
       |
       |ncftp / > set so-bufsize 0
       |
       |ncftp / > get -z large /dev/null
       |/dev/null:                      1.00 GB   21.27 MB/s
       |
       |ncftp / > set so-bufsize 33554432
       |
       |ncftp / > get -z large /dev/null
       |/dev/null:                      1.00 GB   52.70 MB/s
       |
       |
       |
       |For lukemftp (default ftp client for SUSE linux and some
       |other linux flavors) the change of buffer size is done
       |via commands:
       |	sndbuf 33554432
       |	rcvbuf 33554432
       |
       |
       |Example, uploading from NCBI to ftp.YYYYY.mit.edu:
       |
       |ftp> put 100MB 100MB-1
       |local: 100MB remote: 100MB-1
       |229 Entering Extended Passive Mode (|||60355|)
       |150 Opening BINARY mode data connection for 100MB-1
       |100% |**********************|   100 MB    1.53 MB/s    00:00 ETA
       |226 Transfer complete.
       |104857600 bytes sent in 01:05 (1.52 MB/s)
       |ftp> sndbuf 33554432
       |Socket buffer sizes: send 33554432, receive 87380.
       |ftp> rcvbuf 33554432
       |Socket buffer sizes: send 33554432, receive 33554432.
       |ftp> put 100MB 100MB-2
       |local: 100MB remote: 100MB-2
       |229 Entering Extended Passive Mode (|||60469|)
       |150 Opening BINARY mode data connection for 100MB-2
       |100% |**********************|   100 MB    4.52 MB/s    00:00 ETA
       |226 Transfer complete.
       |104857600 bytes sent in 00:23 (4.33 MB/s)
       |
       |Further reading: "Guide to Bulk Data Transfer over a WAN"
       |	http://fasterdata.es.net/
       |
       |We have good experience with lftp on unix: http://lftp.yar.ru/
       |and FileZilla on Windows: http://filezilla.sourceforge.net/
       |
       |P.S.
       |There were reports that recent versions of FileZilla have bundled bloatware,
       |be careful!
       |""".stripMargin

  private val testDir = "pub/clinvar"
  private val testDirContents = List(
    "ClinGen" -> FileType.Directory,
    "ConceptID_history.txt" -> FileType.File,
    "README.txt" -> FileType.File,
    "README_VCF.txt" -> FileType.File,
    "clinvar_public.xsd" -> FileType.Symlink,
    "clinvar_submission.xsd" -> FileType.Symlink,
    "disease_names" -> FileType.File,
    "document_archives" -> FileType.Directory,
    "gene_condition_source_id" -> FileType.File,
    "presentations" -> FileType.Directory,
    "release_notes" -> FileType.Directory,
    "submission_templates" -> FileType.Directory,
    "tab_delimited" -> FileType.Directory,
    "vcf_GRCh37" -> FileType.Directory,
    "vcf_GRCh38" -> FileType.Directory,
    "xml" -> FileType.Directory,
    "xsd_public" -> FileType.Directory,
    "xsd_submission" -> FileType.Directory
  )
  private val testEmptyDir = "pub/README"

  private def getClient(info: FtpConnectionInfo = testInfo): Stream[IO, FtpApi] =
    Stream.resource(FtpApi.build(info, ExecutionContext.global))

  behavior of "FtpApi"

  it should "read remote files" in {
    val bytes = getClient()
      .flatMap(_.readFile(testPath))
      .compile
      .toChunk
      .unsafeRunSync()

    new String(bytes.toArray) shouldBe testContent
  }

  it should "raise a useful error if connecting to a remote site fails" in {
    val badPort = -1
    val bytesOrError = getClient(testInfo.copy(port = badPort))
      .flatMap(_.readFile(testPath))
      .compile
      .toChunk
      .attempt
      .unsafeRunSync()

    bytesOrError.left.value.getCause.getMessage should include(testInfo.host)
    bytesOrError.left.value.getCause.getMessage should include(badPort.toString)
  }

  it should "raise a useful error if logging into a remote site fails" in {
    val badUser = "foouser"
    val bytesOrError = getClient(testInfo.copy(username = badUser))
      .flatMap(_.readFile(testPath))
      .compile
      .toChunk
      .attempt
      .unsafeRunSync()

    bytesOrError.left.value.getCause.getMessage should include(testInfo.host)
    bytesOrError.left.value.getCause.getMessage should include(badUser.toString)
  }

  it should "read ranges of remote files" in {
    val bytes = getClient()
      .flatMap(_.readFile(testPath, fromByte = 27L, untilByte = Some(200L)))
      .compile
      .toChunk
      .unsafeRunSync()

    new String(bytes.toArray) shouldBe testContent.slice(27, 200)
  }

  it should "handle concurrent reads" in {
    val windows = (0L to testContent.length.toLong).sliding(251, 250).toList
    val bytes = getClient().flatMap { ftp =>
      Stream.emits(windows).covary[IO].parEvalMap(windows.length) { window =>
        ftp
          .readFile(testPath, fromByte = window.head, untilByte = Some(window.last))
          .compile
          .toChunk
      }
    }.compile
      .fold(List.empty[Byte]) { (acc, chunk) =>
        acc ++ chunk.toList
      }
      .unsafeRunSync()

    new String(bytes.toArray) shouldBe testContent
  }

  it should "raise a useful error when reading a nonexistent remote file" in {
    val fakePath = "foobar"

    val bytesOrError = getClient()
      .flatMap(_.readFile(fakePath))
      .compile
      .toChunk
      .attempt
      .unsafeRunSync()

    bytesOrError.left.value.getMessage should include(fakePath)
  }

  it should "list remote directories" in {
    val contents = getClient()
      .flatMap(_.listDirectory(testDir))
      .compile
      .toList
      .unsafeRunSync()

    contents should contain theSameElementsAs testDirContents
  }

  it should "not crash on listing empty directories" in {
    val contents = getClient()
      .flatMap(_.listDirectory(testEmptyDir))
      .compile
      .toList
      .unsafeRunSync()

    contents shouldBe Nil
  }

  it should "raise a helpful error if listing a nonexistent directory" in {
    val fakeDir = "foobar"

    val contentsOrError = getClient()
      .flatMap(_.listDirectory(fakeDir))
      .compile
      .toList
      .attempt
      .unsafeRunSync()

    contentsOrError.left.value.getMessage should include(fakeDir)
  }

  it should "raise a helpful error if listing a non-directory" in {
    val contentsOrError = getClient()
      .flatMap(_.listDirectory(testPath))
      .compile
      .toList
      .attempt
      .unsafeRunSync()

    contentsOrError.left.value.getMessage should include(testPath)
  }
}
