package org.broadinstitute.monster.storage.sftp

import java.io.ByteArrayInputStream

import cats.effect.{ContextShift, IO}
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.ExecutionContext

class SftpApiSpec extends FlatSpec with Matchers with MockFactory {

  private implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  private val fakePath = "some/path/to/some/file"
  private val fakeContents = "some text"

  behavior of "SftpApi"

  it should "read remote files" in {
    val fakeSftp = mock[SftpApi.Client]
    (fakeSftp.openRemoteFile _)
      .expects(fakePath, 0L)
      .returning(IO.pure(new ByteArrayInputStream(fakeContents.getBytes())))

    val api = new SftpApi(fakeSftp, ExecutionContext.global)
    val bytes = api.readFile(fakePath).compile.toChunk.unsafeRunSync()

    bytes.toArray shouldBe fakeContents.getBytes()
  }

  it should "start reading remote files at an offset" in {
    val expectedOffset = 2
    val expectedBytes = fakeContents.getBytes().drop(expectedOffset)

    val fakeSftp = mock[SftpApi.Client]
    (fakeSftp.openRemoteFile _)
      .expects(fakePath, expectedOffset.toLong)
      .returning {
        IO.pure(new ByteArrayInputStream(expectedBytes))
      }

    val api = new SftpApi(fakeSftp, ExecutionContext.global)
    val bytes =
      api.readFile(fakePath, expectedOffset.toLong).compile.toChunk.unsafeRunSync()

    bytes.toArray shouldBe expectedBytes
  }

  it should "terminate the remote file stream at an end byte" in {
    val fakeSftp = mock[SftpApi.Client]
    (fakeSftp.openRemoteFile _)
      .expects(fakePath, 0L)
      .returning(IO.pure(new ByteArrayInputStream(fakeContents.getBytes())))

    val api = new SftpApi(fakeSftp, ExecutionContext.global)
    val bytes =
      api
        .readFile(fakePath, untilByte = Some(3L))
        .compile
        .toChunk
        .unsafeRunSync()

    bytes.toArray shouldBe fakeContents.getBytes().slice(0, 3)
  }

  it should "read a custom range of bytes within a remote file" in {
    val expectedOffset = 2
    val expectedBytes = fakeContents.getBytes().drop(expectedOffset)

    val fakeSftp = mock[SftpApi.Client]
    (fakeSftp.openRemoteFile _)
      .expects(fakePath, expectedOffset)
      .returning {
        IO.pure(new ByteArrayInputStream(expectedBytes))
      }

    val api = new SftpApi(fakeSftp, ExecutionContext.global)
    val bytes =
      api
        .readFile(fakePath, expectedOffset.toLong, untilByte = Some(5L))
        .compile
        .toChunk
        .unsafeRunSync()

    bytes.toArray shouldBe expectedBytes.take(3)
  }
}
