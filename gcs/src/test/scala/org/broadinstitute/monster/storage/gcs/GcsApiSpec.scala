package org.broadinstitute.monster.storage.gcs

import cats.effect.{IO, Resource}
import fs2.Stream
import org.http4s._
import org.scalatest.{FlatSpec, Matchers}

class GcsApiSpec extends FlatSpec with Matchers {

  private val bucket = "bucket"
  private val path = "the/path"
  private val getUri =
    uri"https://www.googleapis.com/storage/v1/b/bucket/o/the%2Fpath?alt=media"
  private val bodyText = "Some example body text"
  private val bodyStream = Stream.emits(bodyText.getBytes.toSeq).covary[IO]

  behavior of "GcsApi"

  it should "read entire objects as a stream" in {
    val api = new GcsApi(req => {
      req.method shouldBe Method.GET
      req.uri shouldBe getUri
      req.headers.toList should contain theSameElementsAs List(
        Header("Accept-Encoding", "identity, gzip"),
        Header("Range", "bytes=0-")
      )
      Resource.pure(Response[IO](body = bodyStream))
    })

    api
      .readObject(bucket, path)
      .through(fs2.text.utf8Decode)
      .compile
      .toChunk
      .map(_.toArray[String].mkString(""))
      .unsafeRunSync() shouldBe bodyText
  }

  it should "gunzip compressed data if told to" in {
    val api = new GcsApi(req => {
      req.method shouldBe Method.GET
      req.uri shouldBe getUri
      req.headers.toList should contain theSameElementsAs List(
        Header("Accept-Encoding", "identity, gzip"),
        Header("Range", "bytes=0-")
      )
      Resource.pure {
        Response[IO](
          body = bodyStream.through(fs2.compress.gzip(GcsApi.GunzipBufferSize)),
          headers = Headers.of(Header("Content-Encoding", "gzip"))
        )
      }
    })

    api
      .readObject(bucket, path, gunzipIfNeeded = true)
      .through(fs2.text.utf8Decode)
      .compile
      .toChunk
      .map(_.toArray[String].mkString(""))
      .unsafeRunSync() shouldBe bodyText
  }

  it should "not gunzip uncompressed data" in {
    val api = new GcsApi(req => {
      req.method shouldBe Method.GET
      req.uri shouldBe getUri
      req.headers.toList should contain theSameElementsAs List(
        Header("Accept-Encoding", "identity, gzip"),
        Header("Range", "bytes=0-")
      )
      Resource.pure(Response[IO](body = bodyStream))
    })

    api
      .readObject(bucket, path, gunzipIfNeeded = true)
      .through(fs2.text.utf8Decode)
      .compile
      .toChunk
      .map(_.toArray[String].mkString(""))
      .unsafeRunSync() shouldBe bodyText
  }

  it should "read objects starting at an offset" in {
    val start = 100L
    val api = new GcsApi(req => {
      req.method shouldBe Method.GET
      req.uri shouldBe getUri
      req.headers.toList should contain theSameElementsAs List(
        Header("Accept-Encoding", "identity, gzip"),
        Header("Range", s"bytes=$start-")
      )
      Resource.pure(Response[IO](body = bodyStream))
    })

    api
      .readObject(bucket, path, fromByte = start)
      .through(fs2.text.utf8Decode)
      .compile
      .toChunk
      .map(_.toArray[String].mkString(""))
      .unsafeRunSync() shouldBe bodyText
  }

  it should "read objects ending before the final byte" in {
    val end = 1000L
    val api = new GcsApi(req => {
      req.method shouldBe Method.GET
      req.uri shouldBe getUri
      req.headers.toList should contain theSameElementsAs List(
        Header("Accept-Encoding", "identity, gzip"),
        Header("Range", s"bytes=0-${end - 1}")
      )
      Resource.pure(Response[IO](body = bodyStream))
    })

    api
      .readObject(bucket, path, untilByte = Some(end))
      .through(fs2.text.utf8Decode)
      .compile
      .toChunk
      .map(_.toArray[String].mkString(""))
      .unsafeRunSync() shouldBe bodyText
  }

  it should "read slices in the middle of an object" in {
    val start = 100L
    val end = 1000L
    val api = new GcsApi(req => {
      req.method shouldBe Method.GET
      req.uri shouldBe getUri
      req.headers.toList should contain theSameElementsAs List(
        Header("Accept-Encoding", "identity, gzip"),
        Header("Range", s"bytes=$start-${end - 1}")
      )
      Resource.pure(Response[IO](body = bodyStream))
    })

    api
      .readObject(bucket, path, fromByte = start, untilByte = Some(end))
      .through(fs2.text.utf8Decode)
      .compile
      .toChunk
      .map(_.toArray[String].mkString(""))
      .unsafeRunSync() shouldBe bodyText
  }

  it should "report failure if reading an object returns a 4XX code" in {
    val err =
      s"""OH NO!
         |
         |I broke.""".stripMargin

    val api = new GcsApi({ _ =>
      Resource.pure {
        Response[IO](
          status = Status.BadRequest,
          body = Stream.emit(err).through(fs2.text.utf8Encode)
        )
      }
    })

    api
      .readObject(bucket, path)
      .compile
      .drain
      .attempt
      .map {
        case Right(_) => ???
        case Left(e)  => e.getMessage should include(err)
      }
      .unsafeRunSync()
  }

  it should "report failure if reading an object returns a 5XX code" in {
    val err =
      s"""OH NO!
         |
         |I broke.""".stripMargin

    val api = new GcsApi({ _ =>
      Resource.pure {
        Response[IO](
          status = Status.BadGateway,
          body = Stream.emit(err).through(fs2.text.utf8Encode)
        )
      }
    })

    api
      .readObject(bucket, path)
      .compile
      .drain
      .attempt
      .map {
        case Right(_) => ???
        case Left(e)  => e.getMessage should include(err)
      }
      .unsafeRunSync()
  }
}
