package org.broadinstitute.monster.storage.sftp

import java.io.{ByteArrayInputStream, IOException, InputStream}

import cats.effect.{ContextShift, IO, Resource, Timer}
import net.schmizz.sshj.common.SSHException
import net.schmizz.sshj.sftp.{FileAttributes, FileMode, RemoteResourceInfo}
import org.broadinstitute.monster.storage.common.FileType
import org.scalamock.scalatest.MockFactory
import org.scalatest.{EitherValues, FlatSpec, Matchers}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration

class SftpApiSpec extends FlatSpec with Matchers with MockFactory with EitherValues {

  private implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  private implicit val t: Timer[IO] = IO.timer(ExecutionContext.global)

  private val fakeDir = "some/path/to/some"
  private val fakePath = s"$fakeDir/file"
  private val fakeContents = "some text"

  behavior of "SftpApi"

  it should "read remote files" in {
    val fakeSftp = mock[SftpApi.Client]
    (fakeSftp.openRemoteFile _)
      .expects(fakePath, 0L)
      .returning(Resource.pure(new ByteArrayInputStream(fakeContents.getBytes())))

    val api = new SftpApi(fakeSftp, ExecutionContext.global, 0L, Duration.Zero)
    val bytes = api.readFile(fakePath).compile.toChunk.unsafeRunSync()

    bytes.toArray shouldBe fakeContents.getBytes()
  }

  it should "start reading remote files at an offset" in {
    val expectedOffset = 2
    val expectedBytes = fakeContents.getBytes().drop(expectedOffset)

    val fakeSftp = mock[SftpApi.Client]
    (fakeSftp.openRemoteFile _)
      .expects(fakePath, expectedOffset.toLong)
      .returning(Resource.pure(new ByteArrayInputStream(expectedBytes)))

    val api = new SftpApi(fakeSftp, ExecutionContext.global, 0L, Duration.Zero)
    val bytes =
      api.readFile(fakePath, expectedOffset.toLong).compile.toChunk.unsafeRunSync()

    bytes.toArray shouldBe expectedBytes
  }

  it should "terminate the remote file stream at an end byte" in {
    val fakeSftp = mock[SftpApi.Client]
    (fakeSftp.openRemoteFile _)
      .expects(fakePath, 0L)
      .returning(Resource.pure(new ByteArrayInputStream(fakeContents.getBytes())))

    val api = new SftpApi(fakeSftp, ExecutionContext.global, 0L, Duration.Zero)
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
      .returning(Resource.pure(new ByteArrayInputStream(expectedBytes)))

    val api = new SftpApi(fakeSftp, ExecutionContext.global, 0L, Duration.Zero)
    val bytes =
      api
        .readFile(fakePath, expectedOffset.toLong, untilByte = Some(5L))
        .compile
        .toChunk
        .unsafeRunSync()

    bytes.toArray shouldBe expectedBytes.take(3)
  }

  it should "resume transfers that fail with an SSH error" in {
    val expectedOffset = 2
    val failurePoint = 5

    val inStream1: InputStream = new InputStream {
      private var nextIndex = expectedOffset
      override def read(): Int =
        if (nextIndex == failurePoint) {
          throw new SSHException("test failure")
        } else {
          val toRet = fakeContents.getBytes().apply(nextIndex)
          nextIndex += 1
          toRet.toInt
        }
    }
    val inStream2: InputStream = new InputStream {
      private var nextIndex = failurePoint
      override def read(): Int =
        if (nextIndex == fakeContents.length) {
          -1
        } else {
          val toRet = fakeContents.getBytes().apply(nextIndex)
          nextIndex += 1
          toRet.toInt
        }
    }

    val fakeSftp = mock[SftpApi.Client]
    (fakeSftp.openRemoteFile _)
      .expects(fakePath, expectedOffset.toLong)
      .returning(Resource.pure(inStream1))
    (fakeSftp.openRemoteFile _)
      .expects(fakePath, failurePoint.toLong)
      .returning(Resource.pure(inStream2))

    val api = new SftpApi(fakeSftp, ExecutionContext.global, 1L, Duration.Zero)
    val bytes = api
      .readFile(fakePath, fromByte = expectedOffset.toLong, untilByte = Some(7L))
      .compile
      .toChunk
      .unsafeRunSync()

    bytes.toArray shouldBe fakeContents.getBytes().slice(expectedOffset, 7)
  }

  it should "not resume transfers on non-SSH errors" in {
    val inStream: InputStream = new InputStream {
      private var nextIndex = 0
      override def read(): Int =
        if (nextIndex == 5) {
          throw new IOException("test failure")
        } else {
          val toRet = fakeContents.getBytes().apply(nextIndex)
          nextIndex += 1
          toRet.toInt
        }
    }

    val fakeSftp = mock[SftpApi.Client]
    (fakeSftp.openRemoteFile _)
      .expects(fakePath, 0L)
      .returning(Resource.pure(inStream))

    val api = new SftpApi(fakeSftp, ExecutionContext.global, 1L, Duration.Zero)
    val bytesOrError = api.readFile(fakePath).compile.toChunk.attempt.unsafeRunSync()

    bytesOrError.left.value.getMessage should include(fakePath)
  }

  it should "enforce an upper bound on the number of retries" in {
    val expectedOffset = 2
    val failurePoint = 5

    val inStream1: InputStream = new InputStream {
      private var nextIndex = expectedOffset

      override def read(): Int =
        if (nextIndex == failurePoint) {
          throw new SSHException("test failure")
        } else {
          val toRet = fakeContents.getBytes().apply(nextIndex)
          nextIndex += 1
          toRet.toInt
        }
    }

    val fakeSftp = mock[SftpApi.Client]
    (fakeSftp.openRemoteFile _)
      .expects(fakePath, expectedOffset.toLong)
      .returning(Resource.pure(inStream1))
    (fakeSftp.openRemoteFile _)
      .expects(fakePath, failurePoint.toLong)
      .returning(Resource.pure(inStream1))

    val api = new SftpApi(fakeSftp, ExecutionContext.global, 1L, Duration.Zero)
    val bytesOrError = api
      .readFile(fakePath, fromByte = expectedOffset.toLong)
      .compile
      .toChunk
      .attempt
      .unsafeRunSync()

    bytesOrError.left.value.getMessage should include(fakePath)
  }

  it should "list remote directories" in {
    val fakeContents =
      List(fakePath -> FileMode.Type.REGULAR, s"$fakeDir/dir" -> FileMode.Type.DIRECTORY)
    val fakeSftp = mock[SftpApi.Client]
    (fakeSftp.listRemoteDirectory _).expects(fakeDir).returning {
      IO.pure {
        fakeContents.map {
          case (name, tpe) =>
            val attrs = new FileAttributes.Builder().withType(tpe).build()

            val info = stub[RemoteResourceInfo]
            (info.getPath _).when().returns(name)
            (info.getAttributes _).when().returns(attrs)

            info
        }
      }
    }

    val api = new SftpApi(fakeSftp, ExecutionContext.global, 0L, Duration.Zero)
    val contents = api.listDirectory(fakeDir).compile.toList.unsafeRunSync()
    contents should contain theSameElementsAs List(
      fakePath -> FileType.File,
      s"$fakeDir/dir" -> FileType.Directory
    )
  }

  it should "not break if listing an empty directory" in {
    val fakeSftp = mock[SftpApi.Client]
    (fakeSftp.listRemoteDirectory _).expects(fakeDir).returning(IO.pure(Nil))
    val api = new SftpApi(fakeSftp, ExecutionContext.global, 0L, Duration.Zero)
    val contents = api.listDirectory(fakeDir).compile.toList.unsafeRunSync()
    contents shouldBe empty
  }
}
