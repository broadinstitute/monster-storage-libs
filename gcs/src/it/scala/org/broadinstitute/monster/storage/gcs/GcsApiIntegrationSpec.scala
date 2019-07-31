package org.broadinstitute.monster.storage.gcs

import java.nio.file.{Files, Paths}
import java.time.OffsetDateTime

import cats.effect.{ContextShift, IO, Resource, Timer}
import cats.implicits._
import com.bettercloud.vault.{Vault, VaultConfig}
import com.google.auth.oauth2.ServiceAccountCredentials
import com.google.cloud.storage.{BlobId, BlobInfo, StorageOptions}
import fs2.Stream
import org.apache.commons.codec.digest.DigestUtils
import org.http4s.{MediaType, Status}
import org.http4s.client.blaze.BlazeClientBuilder
import org.http4s.client.middleware.Logger
import org.http4s.headers._
import org.scalatest.{BeforeAndAfterAll, EitherValues, FlatSpec, Matchers}

import scala.concurrent.ExecutionContext

class GcsApiIntegrationSpec
    extends FlatSpec
    with Matchers
    with BeforeAndAfterAll
    with EitherValues {

  private implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  private implicit val t: Timer[IO] = IO.timer(ExecutionContext.global)

  private val bucket = "broad-dsp-monster-dev-integration-test-data"

  private def bodyText(n: Int): Stream[IO, Byte] = {
    Stream
      .randomSeeded(n.toLong)[IO]
      .map(String.valueOf)
      .flatMap(s => Stream.emits(s.getBytes))
      .take(n.toLong)
  }

  // smaller than MaxBytesPerUploadRequest
  private val bodySize = 2 * GcsApi.ChunkSize
  private val body = bodyText(2 * GcsApi.ChunkSize)
  // TODO: best way to get Md5
  private val bodyMd5 = DigestUtils.md5Hex(buildString(body).unsafeRunSync().toString())

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

  private def writeTestFile: Resource[IO, BlobId] = {
    val blobPath = s"test/${OffsetDateTime.now()}/lorem.ipsum"
    val blob = BlobId.of(bucket, blobPath)
    val blobInfo = BlobInfo.newBuilder(blob).setContentType("text/plain").build()

    // TODO best way to get bytes?
    val setup = IO.delay {
      gcsClient.create(blobInfo, buildString(body).unsafeRunSync().getBytes())
      blobInfo.getBlobId
    }

    Resource.make(setup)(id => IO.delay(gcsClient.delete(id)).void)
  }

  private def writeGzippedTestFile: Resource[IO, BlobId] = {
    val blobPath = s"test/${OffsetDateTime.now()}/lorem.ipsum.gz"
    val blob = BlobId.of(bucket, blobPath)
    val blobInfo = BlobInfo
      .newBuilder(blob)
      .setContentType("text/plain")
      .setContentEncoding("gzip")
      .build()

    val setup = body
      .through(fs2.compress.gzip(1024 * 1024))
      .compile
      .toChunk
      .flatMap { zippedBytes =>
        IO.delay(gcsClient.create(blobInfo, zippedBytes.toArray[Byte]))
      }
      .map(_ => blobInfo.getBlobId)

    Resource.make(setup)(id => IO.delay(gcsClient.delete(id)).void)
  }

  private def buildString(bytes: Stream[IO, Byte]): IO[String] =
    bytes
      .through(fs2.text.utf8Decode)
      .compile
      .toChunk
      .map(_.toArray[String].mkString(""))

  behavior of "GcsApi"

  it should "read entire objects as a stream" in {
    writeTestFile.use { blob =>
      withClient(api => buildString(api.readObject(blob.getBucket, blob.getName)))
    }.unsafeRunSync() shouldBe buildString(body).unsafeRunSync()
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
    val readText = writeGzippedTestFile.use { blob =>
      withClient { api =>
        buildString {
          api
            .readObject(blob.getBucket, blob.getName)
            .through(fs2.compress.gunzip(bodySize))
        }
      }
    }

    val expected = buildString(body).unsafeRunSync()
    readText.unsafeRunSync() shouldBe expected
  }

  it should "read gzipped data with client-side decompression" in {
    val readText = writeGzippedTestFile.use { blob =>
      withClient { api =>
        buildString {
          api.readObject(blob.getBucket, blob.getName, gunzipIfNeeded = true)
        }
      }
    }

    val expected = buildString(body).unsafeRunSync()
    readText.unsafeRunSync() shouldBe expected
  }

  it should "no-op client-side decompression on uncompressed data" in {
    val readText = writeGzippedTestFile.use { blob =>
      withClient { api =>
        buildString {
          api.readObject(blob.getBucket, blob.getName, gunzipIfNeeded = true)
        }
      }
    }

    val expected = buildString(body).unsafeRunSync()
    readText.unsafeRunSync() shouldBe expected
  }

  it should "read objects starting at an offset" in {
    val readText = writeTestFile.use { blob =>
      withClient { api =>
        buildString(api.readObject(blob.getBucket, blob.getName, fromByte = 128L))
      }
    }

    val expected = buildString(body.drop(128)).unsafeRunSync()
    readText.unsafeRunSync() shouldBe expected
  }

  it should "read gzipped objects starting at an offset" in {
    val readText = writeGzippedTestFile.use { blob =>
      withClient { api =>
        api.readObject(blob.getBucket, blob.getName, fromByte = 10L).compile.toVector
      }
    }

    val expected = body
      .through(fs2.compress.gzip(bodySize))
      .drop(10)
      .compile
      .toVector
      .unsafeRunSync()
    readText.unsafeRunSync() shouldBe expected
  }

  it should "read objects ending before the final byte" in {
    val readText = writeTestFile.use { blob =>
      withClient { api =>
        buildString(
          api.readObject(blob.getBucket, blob.getName, untilByte = Some(10L))
        )
      }
    }

    val expected = buildString(body.take(10)).unsafeRunSync()
    readText.unsafeRunSync() shouldBe expected
  }

  it should "read gzipped objects ending before the final byte" in {
    val readText = writeGzippedTestFile.use { blob =>
      withClient { api =>
        api
          .readObject(blob.getBucket, blob.getName, untilByte = Some(10L))
          .compile
          .toVector
      }
    }

    val expected = body
      .through(fs2.compress.gzip(GcsApi.ChunkSize))
      .take(10)
      .compile
      .toVector
      .unsafeRunSync()
    readText.unsafeRunSync() shouldBe expected
  }

  it should "read slices in the middle of an object" in {
    val readText = writeTestFile.use { blob =>
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

    // TODO alternaitve to slice(10, 50)?
    val expected = buildString(body.drop(10).take(40)).unsafeRunSync()
    readText.unsafeRunSync() shouldBe expected
  }

  it should "read slices in the middle of a gzipped object" in {
    val readText = writeGzippedTestFile.use { blob =>
      withClient { api =>
        api
          .readObject(blob.getBucket, blob.getName, fromByte = 10L, untilByte = Some(50L))
          .compile
          .toVector
      }
    }

    val expected = body
      .through(fs2.compress.gzip(1024 * 1024))
      .drop(10)
      .take(40)
      .compile
      .toVector
      .unsafeRunSync()
    readText.unsafeRunSync() shouldBe expected
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
    val wasDeleted = writeGzippedTestFile.use { blob =>
      withClient { api =>
        api.deleteObject(blob.getBucket, blob.getName).map { reportedDelete =>
          !gcsExists(blob) && reportedDelete
        }
      }
    }

    wasDeleted.unsafeRunSync() shouldBe true
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
    val bodyMd5 = DigestUtils.md5Hex(buildString(body).unsafeRunSync().toString())

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
    val bodyMd5 = DigestUtils.md5Hex(buildString(body).unsafeRunSync().toString())

    val wasCreated = withClient { api =>
      api
        .createObject(bucket, path, textPlain, bodySize.toLong, Some(bodyMd5), body)
        .bracket(_ => IO.delay(gcsExists(BlobId.of(bucket, path)))) { _ =>
          IO.delay(gcsClient.delete(bucket, path)).as(())
        }
    }

    wasCreated.unsafeRunSync() shouldBe true
  }

  it should "fail to create a new gcs object given an incorrect expected size when the object is larger than the the MaxBytesPerUploadRequest" in {
    val path = s"test/${OffsetDateTime.now()}/foobar"
    val bodySize = GcsApi.MaxBytesPerUploadRequest * 2
    val body = bodyText(bodySize)
    val bodyMd5 = DigestUtils.md5Hex(buildString(body).unsafeRunSync().toString())
    val incorrectExpectedSize = bodySize + GcsApi.MaxBytesPerUploadRequest * 2

    val tryCreate = withClient { api =>
      api
        .createObject(
          bucket,
          path,
          textPlain,
          incorrectExpectedSize.toLong,
          Some(bodyMd5),
          body
        )
    }

    tryCreate.recover {
      case GcsApi.GcsFailure(status, _, _) =>
        status shouldBe Status.BadRequest
        ()
    }.unsafeRunSync()

    if (gcsExists(BlobId.of(bucket, path))) {
      IO.delay(gcsClient.delete(bucket, path)).as(())
      gcsExists(BlobId.of(bucket, path)) shouldBe false
    }
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
    val objectExists = writeGzippedTestFile.use { blob =>
      withClient { api =>
        api.statObject(blob.getBucket, blob.getName).map {
          case (reportsObjectExists, reportedMd5) =>
            gcsExists(blob) && reportsObjectExists && reportedMd5.get == gcsClient
              .get(blob)
              .getMd5
        }
      }
    }

    objectExists.unsafeRunSync() shouldBe true
  }

  it should "check if a GCS object exists and return false" in {
    val path = s"test/${OffsetDateTime.now()}/foobar"

    val objectExists = writeGzippedTestFile.use { blob =>
      withClient { api =>
        api.statObject(blob.getBucket, path).map {
          case (reportsObjectExists, reportedMd5) =>
            !(gcsExists(blob) && reportsObjectExists) && reportedMd5.isEmpty
        }
      }
    }

    objectExists.unsafeRunSync() shouldBe true
  }

  // init and upload
  it should "upload files using resumable uploads" in {
    val path = s"test/${OffsetDateTime.now()}/foobar"

    withClient { api =>
      for {
        uploadToken <- api
          .initResumableUpload(
            bucket,
            path,
            textPlain,
            bodySize.toLong,
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

    withClient { api =>
      for {
        uploadToken <- api
          .initResumableUpload(
            bucket,
            path,
            textPlain,
            bodySize.toLong,
            Some(bodyMd5)
          )
        bytesUploaded <- api
          .uploadBytes(
            bucket,
            uploadToken,
            0,
            body.take(bodySize / 2L)
          )
        numUploaded = bytesUploaded.left.value
        finalOutput <- api.uploadBytes(
          bucket,
          uploadToken,
          numUploaded,
          body.drop(numUploaded)
        )
      } yield {
        bytesUploaded.left.value <= bodySize / 2L shouldBe true
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
}
