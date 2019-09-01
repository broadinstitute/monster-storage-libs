package org.broadinstitute.monster.storage.ftp

import java.io.ByteArrayInputStream

import cats.effect.{ContextShift, IO, Resource}
import org.scalamock.scalatest.MockFactory
import org.scalatest.{EitherValues, FlatSpec, Matchers}

import scala.concurrent.ExecutionContext

class FtpApiSpec extends FlatSpec with Matchers with MockFactory with EitherValues {

  private implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  private val fakeDir = "some/path/to/some"
  private val fakePath = s"$fakeDir/file"
  private val fakeContents = "some text"

  behavior of "FtpApi"

  it should "read remote files" in {
    val fakeFtp = mock[FtpApi.Client]
    (fakeFtp.openRemoteFile _)
      .expects(fakePath, 0L)
      .returning(Resource.pure(Some(new ByteArrayInputStream(fakeContents.getBytes()))))

    val api = new FtpApi(fakeFtp, ExecutionContext.global)
    val bytes = api.readFile(fakePath).compile.toChunk.unsafeRunSync()

    bytes.toArray shouldBe fakeContents.getBytes()
  }

  it should "not crash when reading a nonexistent file" in {
    val fakeFtp = mock[FtpApi.Client]
    (fakeFtp.openRemoteFile _)
      .expects(fakePath, 0L)
      .returning(Resource.pure(None))

    val api = new FtpApi(fakeFtp, ExecutionContext.global)
    val bytesOrError = api.readFile(fakePath).compile.toChunk.attempt.unsafeRunSync()

    bytesOrError.left.value.getMessage should include(fakePath)
  }

  it should "start reading remote files at an offset" in {
    val expectedOffset = 2
    val fakeFtp = mock[FtpApi.Client]
    (fakeFtp.openRemoteFile _)
      .expects(fakePath, expectedOffset.toLong)
      .returning(Resource.pure(Some(new ByteArrayInputStream(fakeContents.getBytes()))))

    val api = new FtpApi(fakeFtp, ExecutionContext.global)
    val bytes = api
      .readFile(fakePath, fromByte = expectedOffset.toLong)
      .compile
      .toChunk
      .unsafeRunSync()

    bytes.toArray shouldBe fakeContents.getBytes()
  }

  it should "terminate the remote file stream at an end byte" in {
    val fakeFtp = mock[FtpApi.Client]
    (fakeFtp.openRemoteFile _)
      .expects(fakePath, 0L)
      .returning(Resource.pure(Some(new ByteArrayInputStream(fakeContents.getBytes()))))

    val api = new FtpApi(fakeFtp, ExecutionContext.global)
    val bytes =
      api.readFile(fakePath, untilByte = Some(3L)).compile.toChunk.unsafeRunSync()

    bytes.toArray shouldBe fakeContents.getBytes().slice(0, 3)
  }

  it should "read a custom range of bytes within a remote file" in {
    val expectedOffset = 2
    val fakeFtp = mock[FtpApi.Client]
    (fakeFtp.openRemoteFile _)
      .expects(fakePath, expectedOffset.toLong)
      .returning {
        Resource.pure(
          Some(new ByteArrayInputStream(fakeContents.getBytes().drop(expectedOffset)))
        )
      }

    val api = new FtpApi(fakeFtp, ExecutionContext.global)
    val bytes = api
      .readFile(fakePath, fromByte = expectedOffset.toLong, untilByte = Some(3L))
      .compile
      .toChunk
      .unsafeRunSync()

    bytes.toArray shouldBe fakeContents.getBytes().slice(expectedOffset, 3)
  }
}
