package org.broadinstitute.monster.storage.ftp

import java.io.{ByteArrayInputStream, IOException, InputStream}

import cats.effect.{Blocker, ContextShift, IO, Resource, Timer}
import org.apache.commons.net.ftp.{FTPConnectionClosedException, FTPFile}
import org.broadinstitute.monster.storage.common.FileType
import org.scalamock.scalatest.MockFactory
import org.scalatest.{EitherValues, FlatSpec, Matchers}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration

class CommonsNetFtpApiSpec
    extends FlatSpec
    with Matchers
    with MockFactory
    with EitherValues {
  private implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  private implicit val t: Timer[IO] = IO.timer(ExecutionContext.global)

  private val fakeDir = "some/path/to/some"
  private val fakePath = s"$fakeDir/file"
  private val fakeContents = "some text"
  private val fakeChunkSize = 128
  private val blocker = Blocker.liftExecutionContext(ExecutionContext.global)

  behavior of "CommonsNetFtpApi"

  it should "read remote files" in {
    val fakeFtp = mock[CommonsNetFtpApi.Client]
    (fakeFtp.openRemoteFile _)
      .expects(fakePath, 0L)
      .returning(Resource.pure(Some(new ByteArrayInputStream(fakeContents.getBytes()))))

    val api = new CommonsNetFtpApi(
      fakeFtp,
      blocker,
      fakeChunkSize,
      0,
      Duration.Zero
    )
    val bytes = api.readFile(fakePath).compile.toChunk.unsafeRunSync()

    bytes.toArray shouldBe fakeContents.getBytes()
  }

  it should "not NPE when reading a nonexistent file" in {
    val fakeFtp = mock[CommonsNetFtpApi.Client]
    (fakeFtp.openRemoteFile _)
      .expects(fakePath, 0L)
      .returning(Resource.pure(None))

    val api = new CommonsNetFtpApi(
      fakeFtp,
      blocker,
      fakeChunkSize,
      0,
      Duration.Zero
    )
    val bytesOrError = api.readFile(fakePath).compile.toChunk.attempt.unsafeRunSync()

    bytesOrError.left.value.getMessage should include(fakePath)
  }

  it should "start reading remote files at an offset" in {
    val expectedOffset = 2
    val fakeFtp = mock[CommonsNetFtpApi.Client]
    (fakeFtp.openRemoteFile _)
      .expects(fakePath, expectedOffset.toLong)
      .returning(Resource.pure(Some(new ByteArrayInputStream(fakeContents.getBytes()))))

    val api = new CommonsNetFtpApi(
      fakeFtp,
      blocker,
      fakeChunkSize,
      0,
      Duration.Zero
    )
    val bytes = api
      .readFile(fakePath, fromByte = expectedOffset.toLong)
      .compile
      .toChunk
      .unsafeRunSync()

    bytes.toArray shouldBe fakeContents.getBytes()
  }

  it should "terminate the remote file stream at an end byte" in {
    val fakeFtp = mock[CommonsNetFtpApi.Client]
    (fakeFtp.openRemoteFile _)
      .expects(fakePath, 0L)
      .returning(Resource.pure(Some(new ByteArrayInputStream(fakeContents.getBytes()))))

    val api = new CommonsNetFtpApi(
      fakeFtp,
      blocker,
      fakeChunkSize,
      0,
      Duration.Zero
    )
    val bytes =
      api.readFile(fakePath, untilByte = Some(3L)).compile.toChunk.unsafeRunSync()

    bytes.toArray shouldBe fakeContents.getBytes().slice(0, 3)
  }

  it should "read a custom range of bytes within a remote file" in {
    val expectedOffset = 2
    val fakeFtp = mock[CommonsNetFtpApi.Client]
    (fakeFtp.openRemoteFile _)
      .expects(fakePath, expectedOffset.toLong)
      .returning {
        Resource.pure(
          Some(new ByteArrayInputStream(fakeContents.getBytes().drop(expectedOffset)))
        )
      }

    val api = new CommonsNetFtpApi(
      fakeFtp,
      blocker,
      fakeChunkSize,
      0,
      Duration.Zero
    )
    val bytes = api
      .readFile(fakePath, fromByte = expectedOffset.toLong, untilByte = Some(3L))
      .compile
      .toChunk
      .unsafeRunSync()

    bytes.toArray shouldBe fakeContents.getBytes().slice(expectedOffset, 3)
  }

  it should "resume transfers if the FTP server terminates the connection (code 421)" in {
    val expectedOffset = 2
    val failurePoint = 5

    val inStream1: InputStream = new InputStream {
      private var nextIndex = expectedOffset
      override def read(): Int =
        if (nextIndex == failurePoint) {
          throw new FTPConnectionClosedException("test failure")
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

    val fakeFtp = mock[CommonsNetFtpApi.Client]
    (fakeFtp.openRemoteFile _)
      .expects(fakePath, expectedOffset.toLong)
      .returning(Resource.pure(Some(inStream1)))
    (fakeFtp.openRemoteFile _)
      .expects(fakePath, failurePoint.toLong)
      .returning(Resource.pure(Some(inStream2)))

    val api = new CommonsNetFtpApi(
      fakeFtp,
      blocker,
      fakeChunkSize,
      1,
      Duration.Zero
    )
    val bytes = api
      .readFile(fakePath, fromByte = expectedOffset.toLong, untilByte = Some(7L))
      .compile
      .toChunk
      .unsafeRunSync()

    bytes.toArray shouldBe fakeContents.getBytes().slice(expectedOffset, 7)
  }

  it should "not resume transfers on errors other than code 421" in {
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

    val fakeFtp = mock[CommonsNetFtpApi.Client]
    (fakeFtp.openRemoteFile _)
      .expects(fakePath, 0L)
      .returning(Resource.pure(Some(inStream)))

    val api = new CommonsNetFtpApi(
      fakeFtp,
      blocker,
      fakeChunkSize,
      1,
      Duration.Zero
    )
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
          throw new FTPConnectionClosedException("test failure")
        } else {
          val toRet = fakeContents.getBytes().apply(nextIndex)
          nextIndex += 1
          toRet.toInt
        }
    }

    val fakeFtp = mock[CommonsNetFtpApi.Client]
    (fakeFtp.openRemoteFile _)
      .expects(fakePath, expectedOffset.toLong)
      .returning(Resource.pure(Some(inStream1)))
    (fakeFtp.openRemoteFile _)
      .expects(fakePath, failurePoint.toLong)
      .returning(Resource.pure(Some(inStream1)))

    val api = new CommonsNetFtpApi(
      fakeFtp,
      blocker,
      fakeChunkSize,
      1,
      Duration.Zero
    )
    val bytesOrError = api
      .readFile(fakePath, fromByte = expectedOffset.toLong)
      .compile
      .toChunk
      .attempt
      .unsafeRunSync()

    bytesOrError.left.value.getMessage should include(fakePath)
  }

  it should "list remote directories" in {
    val fakeDir = stub[FTPFile]
    (fakeDir.isDirectory _).when().returns(true)

    val fakeContents = List.tabulate(10) { i =>
      val toRet = stub[FTPFile]
      (toRet.getName _).when().returns(s"file-$i")
      (toRet.getType _).when().returns(i % 4)
      toRet
    }
    val fakeFtp = mock[CommonsNetFtpApi.Client]
    (fakeFtp.statRemoteFile _).expects(fakePath).returning(IO.pure(Some(fakeDir)))
    (fakeFtp.listRemoteDirectory _).expects(fakePath).returning(IO.pure(fakeContents))

    val api = new CommonsNetFtpApi(
      fakeFtp,
      blocker,
      fakeChunkSize,
      0,
      Duration.Zero
    )
    val contents = api.listDirectory(fakePath).compile.toList.unsafeRunSync()

    contents shouldBe List.tabulate(10) { i =>
      val expectedType = (i % 4) match {
        case FTPFile.DIRECTORY_TYPE     => FileType.Directory
        case FTPFile.FILE_TYPE          => FileType.File
        case FTPFile.SYMBOLIC_LINK_TYPE => FileType.Symlink
        case _                          => FileType.Other
      }
      s"file-$i" -> expectedType
    }
  }

  it should "raise an error when listing a remote file" in {
    val fakeFile = stub[FTPFile]
    (fakeFile.isDirectory _).when().returns(false)

    val fakeFtp = mock[CommonsNetFtpApi.Client]
    (fakeFtp.statRemoteFile _).expects(fakePath).returning(IO.pure(Some(fakeFile)))

    val api = new CommonsNetFtpApi(
      fakeFtp,
      blocker,
      fakeChunkSize,
      0,
      Duration.Zero
    )
    val contentsOrError =
      api.listDirectory(fakePath).compile.toChunk.attempt.unsafeRunSync()

    contentsOrError.left.value.getMessage should include(fakePath)
  }

  it should "raise an error when listing a nonexistent remote directory" in {
    val fakeFtp = mock[CommonsNetFtpApi.Client]
    (fakeFtp.statRemoteFile _).expects(fakePath).returning(IO.pure(None))

    val api = new CommonsNetFtpApi(
      fakeFtp,
      blocker,
      fakeChunkSize,
      0,
      Duration.Zero
    )
    val contentsOrError =
      api.listDirectory(fakePath).compile.toChunk.attempt.unsafeRunSync()

    contentsOrError.left.value.getMessage should include(fakePath)
  }
}
