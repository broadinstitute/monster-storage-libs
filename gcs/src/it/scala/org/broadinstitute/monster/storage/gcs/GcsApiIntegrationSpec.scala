package org.broadinstitute.monster.storage.gcs

import java.nio.file.{Files, Paths}
import java.time.OffsetDateTime

import cats.effect.{ContextShift, IO, Resource, Timer}
import cats.implicits._
import com.bettercloud.vault.{Vault, VaultConfig}
import com.google.auth.oauth2.ServiceAccountCredentials
import com.google.cloud.storage.Storage.BlobListOption
import com.google.cloud.storage.{BlobId, BlobInfo, StorageOptions}
import fs2.{Chunk, Stream}
import org.apache.commons.codec.digest.DigestUtils
import org.broadinstitute.monster.storage.common.FileType
import org.http4s.{MediaType, Status}
import org.http4s.client.blaze.BlazeClientBuilder
import org.http4s.client.middleware.Logger
import org.http4s.headers._
import org.scalatest.{BeforeAndAfterAll, EitherValues, FlatSpec, Matchers}

import scala.concurrent.ExecutionContext
import scala.collection.JavaConverters._

class GcsApiIntegrationSpec
    extends FlatSpec
    with Matchers
    with BeforeAndAfterAll
    with EitherValues {

  private implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  private implicit val t: Timer[IO] = IO.timer(ExecutionContext.global)

  private val bucket = "broad-dsp-monster-dev-integration-test-data"

  private def bodyText(n: Long): Stream[IO, Byte] = {
    Stream
      .randomSeeded(n)[IO]
      .map(String.valueOf)
      .flatMap(s => Stream.emits(s.getBytes))
      .take(n)
  }

  private val writerJson = {
    val tokenPath = sys.env
      .get("VAULT_TOKEN_PATH")
      .fold(Paths.get(sys.env("HOME"), ".vault-token"))(Paths.get(_))

    val config = new VaultConfig()
      .token(new String(Files.readAllBytes(tokenPath)).trim())
      .engineVersion(1)
      .build()

    val json = new Vault(config)
      .logical()
      .read("secret/dsde/monster/dev/integration-test/storage-libs/gcs.json")
      .getDataObject

    val tmp = Files.createTempFile("storage-libs", "integration-test")
    Files.write(tmp, json.toString.getBytes)
  }

  override def afterAll(): Unit = Files.delete(writerJson)

  private val gcsClient = StorageOptions
    .newBuilder()
    .setCredentials(
      ServiceAccountCredentials.fromStream(Files.newInputStream(writerJson))
    )
    .build()
    .getService

  private def withClient[T](run: GcsApi => IO[T]): IO[T] =
    BlazeClientBuilder[IO](ExecutionContext.global).resource.use { http =>
      GcsApi
        .build(Logger(logHeaders = true, logBody = false)(http), Some(writerJson))
        .flatMap(run)
    }

  private def writeTestFile(body: Chunk[Byte]): Resource[IO, BlobId] = {
    val blobPath = s"test/${OffsetDateTime.now()}/lorem.ipsum"
    val blob = BlobId.of(bucket, blobPath)
    val blobInfo = BlobInfo.newBuilder(blob).setContentType("text/plain").build()

    val setup = IO.delay {
      gcsClient.create(blobInfo, body.toArray[Byte])
      blobInfo.getBlobId
    }

    Resource.make(setup)(id => IO.delay(gcsClient.delete(id)).void)
  }

  private def writeGzippedTestFile(body: Chunk[Byte]): Resource[IO, BlobId] = {
    val blobPath = s"test/${OffsetDateTime.now()}/lorem.ipsum.gz"
    val blob = BlobId.of(bucket, blobPath)
    val blobInfo = BlobInfo
      .newBuilder(blob)
      .setContentType("text/plain")
      .setContentEncoding("gzip")
      .build()

    val setup = Stream
      .chunk(body)
      .covary[IO]
      .through(fs2.compress.gzip(1024 * 1024))
      .compile
      .toChunk
      .flatMap { zippedBytes =>
        IO.delay(gcsClient.create(blobInfo, zippedBytes.toArray[Byte]))
      }
      .map(_ => blobInfo.getBlobId)

    Resource.make(setup)(id => IO.delay(gcsClient.delete(id)).void)
  }

  private def writeTestDirectory(
    levelOneCount: Int,
    levelTwoCount: Int
  ): Resource[IO, String] = {
    val blobPrefix = s"test/${OffsetDateTime.now()}"
    val setup = IO.delay {
      (0 until levelOneCount).foreach { i =>
        val blob = BlobId.of(bucket, s"$blobPrefix/lorem-$i.ipsum")
        val blobInfo = BlobInfo.newBuilder(blob).setContentType("text/plain").build()
        gcsClient.create(blobInfo, Array.empty[Byte])
      }
      (0 until levelTwoCount).foreach { j =>
        val blob = BlobId.of(bucket, s"$blobPrefix/level2/lorem-$j.ipsum")
        val blobInfo = BlobInfo.newBuilder(blob).setContentType("text/plain").build()
        gcsClient.create(blobInfo, Array.empty[Byte])
      }
      blobPrefix
    }

    Resource.make(setup) { prefix =>
      IO.delay {
        val allBlobs =
          gcsClient.list(bucket, BlobListOption.prefix(s"$prefix/")).iterateAll().asScala
        gcsClient.delete(allBlobs.map(_.getBlobId).asJava)
        ()
      }
    }
  }

  private def buildString(bytes: Stream[IO, Byte]): IO[String] =
    bytes
      .through(fs2.text.utf8Decode)
      .compile
      .toChunk
      .map(_.toArray[String].mkString(""))

  behavior of "GcsApi"

  it should "read entire objects as a stream" in {
    val checks = for {
      bodyChunk <- bodyText(2L * GcsApi.ChunkSize).compile.toChunk
      writtenBytes <- writeTestFile(bodyChunk).use { blob =>
        withClient(api => buildString(api.readObject(blob.getBucket, blob.getName)))
      }
      _ <- IO.delay(writtenBytes shouldBe new String(bodyChunk.toArray))
    } yield ()

    checks.unsafeRunSync()
  }

  /*
   * For users hosting static content in buckets, GCS offers the
   * convenience of server-side decompression for objects with a
   * content-encoding set to 'gzip'. This feature kicks in automatically
   * on GET requests to any gzipped object unless the client signals
   * that it's OK accepting gzip data.
   *
   * This feature breaks use of the 'Range' header, which is what
   * we use to support reading chunks of data from GCS at a time, so
   * we want to be sure it's always disabled.
   */
  it should "read gzipped data as-is, with no decompression" in {
    val checks = for {
      bodyChunk <- bodyText(2L * GcsApi.ChunkSize).compile.toChunk
      writtenBytes <- writeGzippedTestFile(bodyChunk).use { blob =>
        withClient { api =>
          buildString {
            api
              .readObject(blob.getBucket, blob.getName)
              .through(fs2.compress.gunzip(2 * GcsApi.ChunkSize))
          }
        }
      }
      _ <- IO.delay(writtenBytes shouldBe new String(bodyChunk.toArray))
    } yield ()

    checks.unsafeRunSync()
  }

  it should "read gzipped data with client-side decompression" in {
    val checks = for {
      bodyChunk <- bodyText(2L * GcsApi.ChunkSize).compile.toChunk
      writtenBytes <- writeGzippedTestFile(bodyChunk).use { blob =>
        withClient { api =>
          buildString {
            api.readObject(blob.getBucket, blob.getName, gunzipIfNeeded = true)
          }
        }
      }
      _ <- IO.delay(writtenBytes shouldBe new String(bodyChunk.toArray))
    } yield ()

    checks.unsafeRunSync()
  }

  it should "no-op client-side decompression on uncompressed data" in {
    val checks = for {
      bodyChunk <- bodyText(2L * GcsApi.ChunkSize).compile.toChunk
      writtenBytes <- writeGzippedTestFile(bodyChunk).use { blob =>
        withClient { api =>
          buildString {
            api.readObject(blob.getBucket, blob.getName, gunzipIfNeeded = true)
          }
        }
      }
      _ <- IO.delay(writtenBytes shouldBe new String(bodyChunk.toArray))
    } yield ()

    checks.unsafeRunSync()
  }

  it should "read objects starting at an offset" in {
    val checks = for {
      bodyChunk <- bodyText(2L * GcsApi.ChunkSize).compile.toChunk
      writtenBytes <- writeTestFile(bodyChunk).use { blob =>
        withClient { api =>
          buildString(api.readObject(blob.getBucket, blob.getName, fromByte = 128L))
        }
      }
      _ <- IO.delay(writtenBytes shouldBe new String(bodyChunk.drop(128).toArray))
    } yield ()

    checks.unsafeRunSync()
  }

  it should "read gzipped objects starting at an offset" in {
    val body = bodyText(2L * GcsApi.ChunkSize)

    val checks = for {
      bodyChunk <- body.compile.toChunk
      writtenBytes <- writeGzippedTestFile(bodyChunk).use { blob =>
        withClient { api =>
          api.readObject(blob.getBucket, blob.getName, fromByte = 10L).compile.toVector
        }
      }
      expected <- Stream
        .chunk(bodyChunk)
        .covary[IO]
        .through(fs2.compress.gzip(2 * GcsApi.ChunkSize))
        .drop(10)
        .compile
        .toVector
      _ <- IO.delay(writtenBytes shouldBe expected)
    } yield ()

    checks.unsafeRunSync()
  }

  it should "read objects ending before the final byte" in {
    val checks = for {
      bodyChunk <- bodyText(2L * GcsApi.ChunkSize).compile.toChunk
      writtenBytes <- writeTestFile(bodyChunk).use { blob =>
        withClient { api =>
          buildString(
            api.readObject(blob.getBucket, blob.getName, untilByte = Some(10L))
          )
        }
      }
      _ <- IO.delay(writtenBytes shouldBe new String(bodyChunk.take(10).toArray))
    } yield ()

    checks.unsafeRunSync()
  }

  it should "read gzipped objects ending before the final byte" in {
    val body = bodyText(2L * GcsApi.ChunkSize)

    val checks = for {
      bodyChunk <- body.compile.toChunk
      writtenBytes <- writeGzippedTestFile(bodyChunk).use { blob =>
        withClient { api =>
          api
            .readObject(blob.getBucket, blob.getName, untilByte = Some(10L))
            .compile
            .toVector
        }
      }
      expected <- Stream
        .chunk(bodyChunk)
        .covary[IO]
        .through(fs2.compress.gzip(2 * GcsApi.ChunkSize))
        .take(10)
        .compile
        .toVector
      _ <- IO.delay(writtenBytes shouldBe expected)
    } yield ()

    checks.unsafeRunSync()
  }

  it should "read slices in the middle of an object" in {
    val checks = for {
      bodyChunk <- bodyText(2L * GcsApi.ChunkSize).compile.toChunk
      writtenBytes <- writeTestFile(bodyChunk).use { blob =>
        withClient { api =>
          buildString(
            api.readObject(
              blob.getBucket,
              blob.getName,
              fromByte = 10L,
              untilByte = Some(50L)
            )
          )
        }
      }
      _ <- IO.delay(writtenBytes shouldBe new String(bodyChunk.drop(10).take(40).toArray))
    } yield ()

    checks.unsafeRunSync()
  }

  it should "read slices in the middle of a gzipped object" in {
    val body = bodyText(2L * GcsApi.ChunkSize)

    val checks = for {
      bodyChunk <- body.compile.toChunk
      writtenBytes <- writeGzippedTestFile(bodyChunk).use { blob =>
        withClient { api =>
          api
            .readObject(
              blob.getBucket,
              blob.getName,
              fromByte = 10L,
              untilByte = Some(50L)
            )
            .compile
            .toVector
        }
      }
      expected <- Stream
        .chunk(bodyChunk)
        .covary[IO]
        .through(fs2.compress.gzip(1024 * 1024))
        .drop(10)
        .take(40)
        .compile
        .toVector
      _ <- IO.delay(writtenBytes shouldBe expected)
    } yield ()

    checks.unsafeRunSync()
  }

  it should "report failure if reading an object returns an error code" in {
    val err = withClient(api => buildString(api.readObject(bucket, "foobar"))).attempt
      .unsafeRunSync()

    err.left.value.getMessage should include("404")
  }

  // deleteObject
  private def gcsExists(blob: BlobId): Boolean =
    Option(gcsClient.get(blob)).exists(_.exists())

  it should "delete a GCS object and return true upon success" in {
    val checks = for {
      bodyChunk <- bodyText(2L * GcsApi.ChunkSize).compile.toChunk
      deletedObject <- writeGzippedTestFile(bodyChunk).use { blob =>
        withClient { api =>
          api.deleteObject(blob.getBucket, blob.getName).map { reportedDelete =>
            !gcsExists(blob) && reportedDelete
          }
        }
      }
      _ <- IO.delay(deletedObject shouldBe true)
    } yield ()

    checks.unsafeRunSync()
  }

  it should "return false if deleting a GCS object that doesn't exist" in {
    val wasDeleted = withClient(_.deleteObject(bucket, "foobar"))
    wasDeleted.unsafeRunSync() shouldBe false
  }

  // createObject
  it should " create a new object in GCS when the object is smaller than the MaxBytesPerUploadRequest" in {
    val path = s"test/${OffsetDateTime.now()}/foobar"
    val bodySize = GcsApi.MaxBytesPerUploadRequest / 2
    val body = bodyText(bodySize)
    val bodyMd5 = DigestUtils.md5Hex(buildString(body).unsafeRunSync())

    val wasCreated = withClient { api =>
      api
        .createObject(bucket, path, textPlain, bodySize.toLong, Some(bodyMd5), body)
        .bracket(_ => IO.delay(gcsExists(BlobId.of(bucket, path)))) { _ =>
          IO.delay(gcsClient.delete(bucket, path)).as(())
        }
    }

    wasCreated.unsafeRunSync() shouldBe true
  }

  it should " create a new object in GCS when the object is larger than the the MaxBytesPerUploadRequest" in {
    val path = s"test/${OffsetDateTime.now()}/foobar"
    val bodySize = GcsApi.MaxBytesPerUploadRequest * 2
    val body = bodyText(bodySize)
    val bodyMd5 = DigestUtils.md5Hex(buildString(body).unsafeRunSync())

    val wasCreated = withClient { api =>
      api
        .createObject(bucket, path, textPlain, bodySize.toLong, Some(bodyMd5), body)
        .bracket(_ => IO.delay(gcsExists(BlobId.of(bucket, path)))) { _ =>
          IO.delay(gcsClient.delete(bucket, path)).as(())
        }
    }

    wasCreated.unsafeRunSync() shouldBe true
  }

  it should "fail to create a new gcs object given an incorrect expected Md5 when the object is larger than the the MaxBytesPerUploadRequest" in {
    val path = s"test/${OffsetDateTime.now()}/foobar"
    val bodySize = GcsApi.MaxBytesPerUploadRequest * 2
    val body = bodyText(bodySize)
    val incorrectMd5 = DigestUtils.md5Hex("badMd5")

    val tryCreate = withClient { api =>
      api
        .createObject(
          bucket,
          path,
          textPlain,
          bodySize.toLong,
          Some(incorrectMd5),
          body
        )
    }

    tryCreate.recover {
      case GcsApi.GcsFailure(status, body, _) =>
        status shouldBe Status.BadRequest
        body should include("MD5")
        ()
    }.unsafeRunSync()

    if (gcsExists(BlobId.of(bucket, path))) {
      IO.delay(gcsClient.delete(bucket, path)).as(())
      gcsExists(BlobId.of(bucket, path)) shouldBe false
    }
  }

  it should "fail to create a new gcs object given an incorrect expected Md5 when the object is smaller than the the MaxBytesPerUploadRequest" in {
    val path = s"test/${OffsetDateTime.now()}/foobar"
    val bodySize = GcsApi.MaxBytesPerUploadRequest / 2
    val body = bodyText(bodySize)
    val incorrectMd5 = DigestUtils.md5Hex("badMd5")

    val tryCreate = withClient { api =>
      api
        .createObject(
          bucket,
          path,
          textPlain,
          bodySize.toLong,
          Some(incorrectMd5),
          body
        )
    }

    tryCreate.recover {
      case GcsApi.GcsFailure(status, body, _) =>
        status shouldBe Status.BadRequest
        body should include("MD5")
        ()
    }.unsafeRunSync()

    if (gcsExists(BlobId.of(bucket, path))) {
      IO.delay(gcsClient.delete(bucket, path)).as(())
      gcsExists(BlobId.of(bucket, path)) shouldBe false
    }
  }

  // createObjectOneShot
  private val textPlain = `Content-Type`(MediaType.text.`plain`)

  it should "create an object in GCS in one upload with no expected md5" in {
    val path = s"test/${OffsetDateTime.now()}/foobar"
    val body = bodyText(2L * GcsApi.ChunkSize)

    val wasCreated = withClient { api =>
      api
        .createObjectOneShot(bucket, path, textPlain, None, body)
        .bracket(_ => IO.delay(gcsExists(BlobId.of(bucket, path)))) { _ =>
          IO.delay(gcsClient.delete(bucket, path)).as(())
        }
    }

    wasCreated.unsafeRunSync() shouldBe true
  }

  it should "create an object in GCS in one upload with a correct expected md5" in {
    val path = s"test/${OffsetDateTime.now()}/foobar"
    val body = bodyText(2L * GcsApi.ChunkSize)
    val bodyMd5 = DigestUtils.md5Hex(buildString(body).unsafeRunSync())

    val wasCreated = withClient { api =>
      api
        .createObjectOneShot(
          bucket,
          path,
          textPlain,
          Some(bodyMd5),
          body
        )
        .bracket(_ => IO.delay(gcsExists(BlobId.of(bucket, path)))) { _ =>
          IO.delay(gcsClient.delete(bucket, path)).as(())
        }
    }

    wasCreated.unsafeRunSync() shouldBe true
  }

  it should "fail to create an object in GCS in one upload with an incorrect md5" in {
    val path = s"test/${OffsetDateTime.now()}/foobar"
    val body = bodyText(2L * GcsApi.ChunkSize)

    val tryCreate = withClient { api =>
      api.createObjectOneShot(
        bucket,
        path,
        textPlain,
        Some(DigestUtils.md5Hex("abcdef1234567890")),
        body
      )
    }

    tryCreate.recover {
      case GcsApi.GcsFailure(status, body, _) =>
        status shouldBe Status.BadRequest
        body should include("MD5")
        ()
    }.unsafeRunSync()

    if (gcsExists(BlobId.of(bucket, path))) {
      IO.delay(gcsClient.delete(bucket, path)).as(())
      gcsExists(BlobId.of(bucket, path)) shouldBe false
    }
  }

  it should "check if a GCS object exists and return true with an md5" in {
    val checks = for {
      bodyChunk <- bodyText(2L * GcsApi.ChunkSize).compile.toChunk
      objectExists <- writeGzippedTestFile(bodyChunk).use { blob =>
        withClient { api =>
          api.statObject(blob.getBucket, blob.getName).map {
            case (reportsObjectExists, reportedMd5) =>
              gcsExists(blob) && reportsObjectExists && reportedMd5.get == gcsClient
                .get(blob)
                .getMd5
          }
        }
      }
      _ <- IO.delay(objectExists shouldBe true)
    } yield ()

    checks.unsafeRunSync()
  }

  it should "check if a GCS object exists and return false" in {
    val path = s"test/${OffsetDateTime.now()}/foobar"

    val checks = for {
      bodyChunk <- bodyText(2L * GcsApi.ChunkSize).compile.toChunk
      objectExists <- writeGzippedTestFile(bodyChunk).use { blob =>
        withClient { api =>
          api.statObject(blob.getBucket, path).map {
            case (reportsObjectExists, reportedMd5) =>
              !(gcsExists(blob) && reportsObjectExists) && reportedMd5.isEmpty
          }
        }
      }
      _ <- IO.delay(objectExists shouldBe true)
    } yield ()

    checks.unsafeRunSync()
  }

  // init and upload
  it should "upload files using resumable uploads" in {
    val path = s"test/${OffsetDateTime.now()}/foobar"
    val body = bodyText(2L * GcsApi.ChunkSize)
    val bodyMd5 = DigestUtils.md5Hex(buildString(body).unsafeRunSync())

    withClient { api =>
      for {
        uploadToken <- api
          .initResumableUpload(
            bucket,
            path,
            textPlain,
            2L * GcsApi.ChunkSize,
            Some(bodyMd5)
          )
        output <- api.uploadBytes(
          bucket,
          uploadToken,
          0,
          body
        )
      } yield {
        output
      }
    }.bracket { output =>
      IO.delay {
        output.isRight shouldBe true
        val blobId = BlobId.of(bucket, path)
        gcsExists(blobId) shouldBe true
        gcsClient.get(blobId).getContent() shouldBe buildString(body)
          .unsafeRunSync()
          .getBytes()
      }
    } { _ =>
      IO.delay(gcsClient.delete(bucket, path)).as(())
    }.unsafeRunSync()
  }

  it should "upload files using resumable uploads over multiple upload calls" in {
    val path = s"test/${OffsetDateTime.now()}/foobar"
    val body = bodyText(2L * GcsApi.ChunkSize)
    val bodyMd5 = DigestUtils.md5Hex(buildString(body).unsafeRunSync())

    withClient { api =>
      for {
        uploadToken <- api
          .initResumableUpload(
            bucket,
            path,
            textPlain,
            2L * GcsApi.ChunkSize,
            Some(bodyMd5)
          )
        bytesUploaded <- api
          .uploadBytes(
            bucket,
            uploadToken,
            0,
            body.take(GcsApi.ChunkSize.toLong)
          )
        numUploaded = bytesUploaded.left.value
        finalOutput <- api.uploadBytes(
          bucket,
          uploadToken,
          numUploaded,
          body.drop(numUploaded)
        )
      } yield {
        bytesUploaded.left.value <= GcsApi.ChunkSize shouldBe true
        finalOutput
      }
    }.bracket { output =>
      IO.delay {
        output.isRight shouldBe true
        val blobId = BlobId.of(bucket, path)
        gcsExists(blobId) shouldBe true
        gcsClient.get(blobId).getContent() shouldBe buildString(body)
          .unsafeRunSync()
          .getBytes()
      }
    } { _ =>
      IO.delay(gcsClient.delete(bucket, path)).as(())
    }.unsafeRunSync()
  }

  it should "report failure if attempting to upload to an uninitialized ID" in {
    val body = bodyText(2L * GcsApi.ChunkSize)

    val tryInitAndUpload = withClient { api =>
      api.uploadBytes(
        bucket,
        "bad-upload-token-plus-kobe",
        0,
        body
      )
    }

    tryInitAndUpload.attempt.unsafeRunSync().left.value should matchPattern {
      case GcsApi.GcsFailure(status, _, _) if status == Status.NotFound => ()
    }
  }

  it should "report failure if data uploaded in a resumable upload doesn't match the expected md5" in {
    val path = s"test/${OffsetDateTime.now()}/foobar"
    val badText = "This is bad text"
    val badMd5 = DigestUtils.md5Hex("This text is different")
    val badTextSize = badText.getBytes().length

    val tryUpload = withClient { api =>
      for {
        uploadToken <- api.initResumableUpload(
          bucket,
          path,
          textPlain,
          badTextSize.toLong,
          Some(badMd5)
        )
        _ <- api.uploadBytes(
          bucket,
          uploadToken,
          0,
          Stream.emits(badText.getBytes).covary[IO]
        )
      } yield ()
    }

    tryUpload.attempt.unsafeRunSync().left.value should matchPattern {
      case GcsApi.GcsFailure(status, _, _) if status == Status.BadRequest => ()
    }

    if (gcsExists(BlobId.of(bucket, path))) {
      IO.delay(gcsClient.delete(bucket, path)).as(())
      gcsExists(BlobId.of(bucket, path)) shouldBe false
    }
  }

  it should "list directory contents" in {
    writeTestDirectory(10, 10).use { directory =>
      withClient(_.listContents(bucket, s"$directory/", 20).compile.toList).map {
        listResults =>
          val expected = (s"$directory/level2/" -> FileType.Directory) :: List.tabulate(
            10
          ) { i =>
            s"$directory/lorem-$i.ipsum" -> FileType.File
          }

          listResults should contain theSameElementsAs expected
      }
    }.unsafeRunSync()
  }

  it should "not break if there are no 'directories' under a listed path" in {
    writeTestDirectory(10, 0).use { directory =>
      withClient(_.listContents(bucket, s"$directory/", 20).compile.toList).map {
        listResults =>
          val expected =
            List.tabulate(10)(i => s"$directory/lorem-$i.ipsum" -> FileType.File)
          listResults should contain theSameElementsAs expected
      }
    }.unsafeRunSync()
  }

  it should "not break if there are only 'directories' under a listed path" in {
    writeTestDirectory(0, 10).use { directory =>
      withClient(_.listContents(bucket, s"$directory/", 20).compile.toList).map {
        _ should contain only (s"$directory/level2/" -> FileType.Directory)
      }
    }.unsafeRunSync()
  }

  it should "read all pages of list results" in {
    writeTestDirectory(20, 0).use { directory =>
      withClient(_.listContents(bucket, s"$directory/", 3).compile.toList).map {
        listResults =>
          val expected = List.tabulate(20) { i =>
            s"$directory/lorem-$i.ipsum" -> FileType.File
          }

          listResults should contain theSameElementsAs expected
      }
    }.unsafeRunSync()
  }

  it should "raise a helpful error on listing a nonexistent path" in {
    val contentsOrError =
      withClient(_.listContents(bucket, "foo", 10).compile.toList.attempt).unsafeRunSync()
    contentsOrError.left.value.getMessage should include("foo")
  }

  it should "copy objects in cloud storage" in {
    val checks = for {
      bodyChunk <- bodyText(2L * GcsApi.ChunkSize).compile.toChunk
      copiedBytes <- writeTestFile(bodyChunk).use { blob =>
        val copyTarget = s"copied/${blob.getName}"
        for {
          _ <- withClient(
            _.copyObject(
              sourceBucket = bucket,
              sourcePath = blob.getName,
              targetBucket = bucket,
              targetPath = copyTarget,
              forceCompletion = true,
              prevToken = None
            )
          )
          bytes <- IO.delay(new String(gcsClient.get(bucket, copyTarget).getContent()))
          _ <- IO.delay(gcsClient.delete(bucket, copyTarget))
        } yield {
          bytes
        }
      }
      _ <- IO.delay(copiedBytes shouldBe new String(bodyChunk.toArray))
    } yield ()

    checks.unsafeRunSync()
  }
}
