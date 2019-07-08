package org.broadinstitute.monster.storage.gcs

import java.nio.file.{Files, Paths}
import java.time.OffsetDateTime

import cats.effect.{ContextShift, IO, Resource, Timer}
import com.bettercloud.vault.{Vault, VaultConfig}
import com.google.auth.oauth2.ServiceAccountCredentials
import com.google.cloud.storage.{BlobId, BlobInfo, StorageOptions}
import org.http4s.client.blaze.BlazeClientBuilder
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.concurrent.ExecutionContext

class GcsApiIntegrationSpec extends FlatSpec with Matchers with BeforeAndAfterAll {

  private implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  private implicit val t: Timer[IO] = IO.timer(ExecutionContext.global)

  private val bucket = "broad-dsp-monster-dev-integration-test-data"

  private val bodyText =
    s"""Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor
       |incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud
       |exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute
       |irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla
       |pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia
       |deserunt mollit anim id est laborum.""".stripMargin

  private val writerJson = {
    val tokenPath = sys.env
      .get("VAULT_TOKEN_PATH")
      .fold(Paths.get(sys.env("HOME"), ".vault-token"))(Paths.get(_))

    val config = new VaultConfig()
      .token(new String(Files.readAllBytes(tokenPath)))
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

  private val authProvider = GcsAuthProvider.build(Some(writerJson))

  private val gcsClient = StorageOptions
    .newBuilder()
    .setCredentials(
      ServiceAccountCredentials.fromStream(Files.newInputStream(writerJson))
    )
    .build()
    .getService

  behavior of "GcsApi"

  it should "read entire objects as a stream" in {
    val blobPath = s"test/${OffsetDateTime.now()}/lorem.ipsum"
    val blob = BlobId.of(bucket, blobPath)
    val blobInfo = BlobInfo.newBuilder(blob).setContentType("text/plain").build()

    val setup = IO.delay {
      gcsClient.create(blobInfo, bodyText.getBytes)
      ()
    }
    val teardown = IO.delay {
      gcsClient.delete(blob)
      ()
    }

    val readText = Resource
      .make(setup)(_ => teardown)
      .flatMap(_ => BlazeClientBuilder[IO](ExecutionContext.global).resource)
      .use { httpClient =>
        authProvider.map(new GcsApi(_, httpClient.stream)).flatMap { api =>
          api
            .readObject(bucket, blobPath)
            .through(fs2.text.utf8Decode)
            .compile
            .toChunk
            .map(_.toArray[String].mkString(""))
        }
      }

    readText.unsafeRunSync() shouldBe bodyText
  }

  it should "read objects starting at an offset" in {
    ???
  }

  it should "read objects ending before the final byte" in {
    ???
  }

  it should "read slices in the middle of an object" in {
    ???
  }

  it should "report failure if reading an object returns an error code" in {
    ???
  }
}