package org.broadinstitute.monster.storage.sftp

import cats.effect.{ContextShift, IO}
import fs2.Stream
import org.scalatest.{EitherValues, FlatSpec, Matchers}

import scala.concurrent.ExecutionContext

class SftpApiIntegrationSpec extends FlatSpec with Matchers with EitherValues {

  private implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

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

  behavior of "SftpApi"

  it should "read remote files" in {
    val bytes = Stream
      .resource(SftpApi.build(testLogin, ExecutionContext.global))
      .flatMap(_.readFile(testPath))
      .compile
      .toChunk
      .unsafeRunSync()

    new String(bytes.toArray) shouldBe testContent
  }

  it should "read ranges of remote files" in {
    val bytes = Stream
      .resource(SftpApi.build(testLogin, ExecutionContext.global))
      .flatMap(_.readFile(testPath, fromByte = 2L, untilByte = Some(7L)))
      .compile
      .toChunk
      .unsafeRunSync()

    new String(bytes.toArray) shouldBe testContent.slice(2, 7)
  }

  it should "handle concurrent reads" in {
    val windows = (0L to testContent.length.toLong).sliding(26, 25).toList
    val bytes =
      Stream
        .resource(SftpApi.build(testLogin, ExecutionContext.global))
        .flatMap { sftp =>
          Stream.emits(windows).covary[IO].parEvalMap(windows.length) { window =>
            sftp
              .readFile(testPath, fromByte = window.head, untilByte = Some(window.last))
              .compile
              .toChunk
          }
        }
        .compile
        .fold(List.empty[Byte]) { (acc, chunk) =>
          acc ++ chunk.toList
        }
        .unsafeRunSync()

    new String(bytes.toArray) shouldBe testContent
  }

  it should "raise a useful error when reading a nonexistent remote file" in {
    val fakePath = "foobar"

    val bytesOrError = Stream
      .resource(SftpApi.build(testLogin, ExecutionContext.global))
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
}
