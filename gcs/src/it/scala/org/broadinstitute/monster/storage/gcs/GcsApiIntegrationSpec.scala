package org.broadinstitute.monster.storage.gcs

import java.nio.file.{Files, Paths}
import java.time.OffsetDateTime

import cats.effect.{ContextShift, IO, Resource, Timer}
import cats.implicits._
import com.bettercloud.vault.{Vault, VaultConfig}
import com.google.auth.oauth2.ServiceAccountCredentials
import com.google.cloud.storage.{BlobId, BlobInfo, StorageOptions}
import fs2.Stream
import org.http4s.client.blaze.BlazeClientBuilder
import org.http4s.client.middleware.Logger
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

  /*
   * NOTE: This is lorem ipsum because it needs to be large enough to meaningfully test
   * GCS's range downloads, and I was too lazy to come up with my own paragraph of text.
   */
  private val bodyText =
    s"""Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor
       |incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud
       |exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute
       |irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla
       |pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia
       |deserunt mollit anim id est laborum.
       |""".stripMargin

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

    val setup = IO.delay {
      gcsClient.create(blobInfo, bodyText.getBytes)
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
      .setContentEncoding("gzip")
      .build()

    val setup = Stream
      .emits(bodyText.getBytes)
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
    }.unsafeRunSync() shouldBe bodyText
  }

  it should "read gzipped data as-is, with no server-side decompression" in {
    writeGzippedTestFile.use { blob =>
      withClient { api =>
        buildString {
          api
            .readObject(blob.getBucket, blob.getName)
            .through(fs2.compress.gunzip(1024 * 1024))
        }
      }
    }.unsafeRunSync() shouldBe bodyText
  }

  it should "read objects starting at an offset" in {
    writeTestFile.use { blob =>
      withClient { api =>
        buildString(api.readObject(blob.getBucket, blob.getName, fromByte = 128L))
      }
    }.unsafeRunSync() shouldBe new String(bodyText.getBytes.drop(128))
  }

  it should "read gzipped objects starting at an offset" in {
    val readText = writeGzippedTestFile.use { blob =>
      withClient { api =>
        api.readObject(blob.getBucket, blob.getName, fromByte = 10L).compile.toVector
      }
    }

    val expected = Stream
      .emits(bodyText.getBytes)
      .through(fs2.compress.gzip(1024 * 1024))
      .drop(10)
      .compile
      .toVector

    readText.unsafeRunSync() shouldBe expected
  }

  it should "read objects ending before the final byte" in {
    writeTestFile.use { blob =>
      withClient { api =>
        buildString(
          api.readObject(blob.getBucket, blob.getName, untilByte = Some(10L))
        )
      }
    }.unsafeRunSync() shouldBe new String(bodyText.getBytes.take(10))
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

    val expected = Stream
      .emits(bodyText.getBytes)
      .through(fs2.compress.gzip(1024 * 1024))
      .take(10)
      .compile
      .toVector

    readText.unsafeRunSync() shouldBe expected
  }

  it should "read slices in the middle of an object" in {
    writeTestFile.use { blob =>
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
    }.unsafeRunSync() shouldBe new String(bodyText.getBytes.slice(10, 50))
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

    val expected = Stream
      .emits(bodyText.getBytes)
      .through(fs2.compress.gzip(1024 * 1024))
      .drop(10)
      .take(40)
      .compile
      .toVector

    readText.unsafeRunSync() shouldBe expected
  }

  it should "report failure if reading an object returns an error code" in {
    val err = withClient(api => buildString(api.readObject(bucket, "foobar"))).attempt
      .unsafeRunSync()

    err.left.value.getMessage should include("404")
  }
}
