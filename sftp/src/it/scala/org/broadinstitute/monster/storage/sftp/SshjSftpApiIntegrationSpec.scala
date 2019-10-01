package org.broadinstitute.monster.storage.sftp

import cats.effect.{Blocker, ContextShift, IO, Timer}
import fs2.Stream
import org.broadinstitute.monster.storage.common.{FileAttributes, FileType}
import org.scalatest.{EitherValues, FlatSpec, Matchers, OptionValues}

import scala.concurrent.ExecutionContext

class SshjSftpApiIntegrationSpec
    extends FlatSpec
    with Matchers
    with EitherValues
    with OptionValues {

  private implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  private implicit val t: Timer[IO] = IO.timer(ExecutionContext.global)

  // FIXME: Change this to point to our own SFTP server once we have one up and running.
  private val testLogin =
    SftpLoginInfo("test.rebex.net", 22, "demo", "password")
  private val testPath = "pub/example/readme.txt"

  // NOTE: mkString used instead of a multi-line string so we can force the Windows line endings
  private val testContent = List(
    "Welcome,",
    "",
    "you are connected using an FTP account used for testing purposes by Rebex FTP for .NET and Rebex FTP/SSL for .NET sample code.",
    "Only read access is allowed and download speed is limited to 16KBps.",
    "",
    "For infomation about Rebex FTP, Rebex FTP/SSL and other Rebex .NET components, please visit our website at http://www.rebex.net/",
    "",
    "For feedback and support, contact support@rebex.cz",
    "",
    "Thanks!",
    ""
  ).mkString("\r\n")

  def getClient(info: SftpLoginInfo = testLogin): Stream[IO, SftpApi] =
    Stream.resource(
      SshjSftpApi.build(info, Blocker.liftExecutionContext(ExecutionContext.global))
    )

  behavior of "SftpApi"

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
    val bytesOrError = getClient(testLogin.copy(port = badPort))
      .flatMap(_.readFile(testPath))
      .compile
      .toChunk
      .attempt
      .unsafeRunSync()

    bytesOrError.left.value.getCause.getMessage should include(testLogin.host)
    bytesOrError.left.value.getCause.getMessage should include(badPort.toString)
  }

  it should "raise a useful error if logging into a remote site fails" in {
    val badUser = "foouser"
    val bytesOrError = getClient(testLogin.copy(username = badUser))
      .flatMap(_.readFile(testPath))
      .compile
      .toChunk
      .attempt
      .unsafeRunSync()

    bytesOrError.left.value.getCause.getMessage should include(testLogin.host)
    bytesOrError.left.value.getCause.getMessage should include(badUser)
  }

  it should "read ranges of remote files" in {
    val bytes = getClient()
      .flatMap(_.readFile(testPath, fromByte = 2L, untilByte = Some(7L)))
      .compile
      .toChunk
      .unsafeRunSync()

    new String(bytes.toArray) shouldBe testContent.slice(2, 7)
  }

  it should "handle concurrent reads" in {
    val windows = (0L to testContent.length.toLong).sliding(26, 25).toList
    val bytes = getClient().flatMap { sftp =>
      Stream.emits(windows).covary[IO].parEvalMap(windows.length) { window =>
        sftp
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

    val errMessage = bytesOrError.left.value.getMessage
    val errCause = bytesOrError.left.value.getCause
    errMessage should include(fakePath)
    errCause.getMessage should include("not found")
  }

  it should "stat remote files" in {
    val attributes =
      getClient().evalMap(_.statFile(testPath)).compile.lastOrError.unsafeRunSync()

    attributes.value shouldBe FileAttributes(testContent.length.toLong, None)
  }

  it should "not break when stat-ing nonexistent remote files" in {
    val attributes =
      getClient().evalMap(_.statFile("foobar")).compile.lastOrError.unsafeRunSync()

    attributes shouldBe None
  }

  it should "list directories" in {
    val listed = getClient()
      .flatMap(_.listDirectory(""))
      .compile
      .toList
      .unsafeRunSync()

    listed should contain allElementsOf List(
      "pub" -> FileType.Directory,
      "readme.txt" -> FileType.File
    )
  }

  it should "preserve full paths when listing" in {
    val listed = getClient()
      .flatMap(_.listDirectory("pub/example"))
      .compile
      .toList
      .unsafeRunSync()

    listed should contain(testPath -> FileType.File)
  }

  it should "raise a useful error when listing a non-directory" in {
    val listedOrErr = getClient()
      .flatMap(_.listDirectory(testPath))
      .compile
      .toChunk
      .attempt
      .unsafeRunSync()

    listedOrErr.left.value.getMessage should include(testPath)
  }
}
