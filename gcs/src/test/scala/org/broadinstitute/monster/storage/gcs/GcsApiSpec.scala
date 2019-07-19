package org.broadinstitute.monster.storage.gcs

import cats.effect.{IO, Resource}
import cats.implicits._
import fs2.Stream
import org.apache.commons.codec.digest.DigestUtils
import org.http4s._
import org.http4s.headers._
import org.http4s.implicits._
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable.ArrayBuffer

class GcsApiSpec extends FlatSpec with Matchers {

  private val bucket = "bucket"
  private val path = "the/path"
  private val uploadToken = "upload-token"

  // numberOfChunks of the bodyText MUST BE SMALLER THAN bodyTextNumberOfBytes
  private val bodyText = "Some example body text"
  private val bodyStream = Stream.emits(bodyText.getBytes.toSeq).covary[IO]
  private val bodyTextNumberOfBytes = bodyText.getBytes().length
  private val numberOfChunks = 4
  private val theMd5 = DigestUtils.md5Hex(bodyText)

  private val baseUri = GcsApi.baseGcsUri(bucket, path)
  private val readObjectURI = baseUri.copy(query = Query.fromPairs("alt" -> "media"))
  private val statObjectURI = baseUri.copy(query = Query.fromPairs("alt" -> "json"))
  private val initResumableUploadURI = GcsApi.baseGcsUploadUri(bucket, "resumable")
  private val uploadURI =
    GcsApi
      .baseGcsUploadUri(bucket, "resumable")
      .copy(
        query = Query.fromPairs("uploadType" -> "resumable", "upload_id" -> uploadToken)
      )
  private val createObjectURI = GcsApi.baseGcsUploadUri(bucket, "multipart")

  private val acceptEncodingHeader = Header("Accept-Encoding", "identity, gzip")

  behavior of "GcsApi"

  // readObject
  it should "read entire objects as a stream" in {
    val api = new GcsApi(req => {
      req.method shouldBe Method.GET
      req.uri shouldBe readObjectURI
      req.headers.toList should contain theSameElementsAs List(
        acceptEncodingHeader,
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
      req.uri shouldBe readObjectURI
      req.headers.toList should contain theSameElementsAs List(
        acceptEncodingHeader,
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
      req.uri shouldBe readObjectURI
      req.headers.toList should contain theSameElementsAs List(
        acceptEncodingHeader,
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
      req.uri shouldBe readObjectURI
      req.headers.toList should contain theSameElementsAs List(
        acceptEncodingHeader,
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
      req.uri shouldBe readObjectURI
      req.headers.toList should contain theSameElementsAs List(
        acceptEncodingHeader,
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
      req.uri shouldBe readObjectURI
      req.headers.toList should contain theSameElementsAs List(
        acceptEncodingHeader,
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

  // statObject
  it should "return true if a GCS object exists" in {
    val api = new GcsApi(req => {
      req.method shouldBe Method.GET
      req.uri shouldBe statObjectURI
      Resource.pure(Response[IO](body = Stream.emits("{}".getBytes())))
    })

    api
      .statObject(bucket, path)
      .unsafeRunSync() shouldBe (true -> None)
  }

  it should "return false if a GCS object does not exist" in {
    val api = new GcsApi(req => {
      req.method shouldBe Method.GET
      req.uri shouldBe statObjectURI
      Resource.pure(Response[IO](status = Status.NotFound))
    })

    api
      .statObject(bucket, path)
      .unsafeRunSync() shouldBe (false -> None)
  }

  it should "return the md5 of an existing object" in {
    val api = new GcsApi(req => {
      req.method shouldBe Method.GET
      req.uri shouldBe statObjectURI
      Resource.pure(
        Response[IO](
          body = Stream.emits(s"""{"${GcsApi.ObjectMd5Key}": "$theMd5"}""".getBytes())
        )
      )
    })

    api
      .statObject(bucket, path)
      .unsafeRunSync() shouldBe (true -> Some(theMd5))
  }

  // createObject
  it should "create a GCS object" in {
    val api = new GcsApi(req => {
      req.method shouldBe Method.POST
      req.uri shouldBe createObjectURI

      Resource.pure(
        Response[IO](
          status = Status.Ok
        )
      )
    })

    api
      .createObject(
        bucket,
        path,
        `Content-Type`(MediaType.`text/event-stream`),
        Some(theMd5),
        bodyStream
      )
      .unsafeRunSync()
  }

  // deleteObject
  it should "delete a GCS object" in {
    val api = new GcsApi(req => {
      req.method shouldBe Method.DELETE
      req.uri shouldBe baseUri
      Resource.pure(
        Response[IO](
          status = Status.Ok
        )
      )
    })

    api
      .deleteObject(bucket, path)
      .unsafeRunSync()
  }

  // initResumableUpload
  it should "initialize a resumable upload" in {
    val api = new GcsApi(req => {
      val checks = req.body.compile.toChunk.map {
        bodyChunk =>
          req.method shouldBe Method.POST
          req.uri shouldBe initResumableUploadURI
          req.headers.toList should contain theSameElementsAs List(
            `Content-Length`.unsafeFromLong(bodyChunk.size.toLong),
            `Content-Type`(MediaType.application.json, Charset.`UTF-8`),
            Header(GcsApi.uploadContentLengthHeaderName, bodyTextNumberOfBytes.toString),
            Header(GcsApi.uploadContentTypeHeaderName, "text/event-stream")
          )
      }

      Resource.liftF(checks).map { _ =>
        Response[IO](
          status = Status.Ok,
          headers = Headers.of(
            Header(GcsApi.uploaderIDHeaderName, uploadToken)
          )
        )
      }
    })

    api
      .initResumableUpload(
        bucket,
        path,
        `Content-Type`(MediaType.`text/event-stream`),
        bodyTextNumberOfBytes.toLong,
        Some(theMd5)
      )
      .unsafeRunSync() shouldBe uploadToken
  }

  // uploadBytes
  it should "upload bytes to a resumable upload in a single chunk" in {
    val api = new GcsApi(req => {
      val contentRangeStart = 0
      val contentRangeEnd = bodyTextNumberOfBytes - 1
      val checks = req.body.compile.toChunk.map { chunk =>
        req.method shouldBe Method.PUT
        req.uri shouldBe uploadURI
        req.headers.toList should contain theSameElementsAs List(
          `Content-Length`.unsafeFromLong(chunk.size.toLong),
          `Content-Range`(contentRangeStart.toLong, contentRangeEnd.toLong)
        )
      }

      Resource.liftF(checks).map { _ =>
        Response[IO](
          status = Status.Ok,
          headers = Headers.of(
            Header("Range", s"bytes 0-${bodyTextNumberOfBytes - 1}/*")
          )
        )
      }
    })

    api
      .uploadBytes(
        bucket,
        uploadToken,
        0,
        bodyStream,
        bodyTextNumberOfBytes * 2
      )
      .unsafeRunSync() shouldBe Right(())
  }

  it should "upload bytes to a resumable upload for a single chunk, without finishing" in {
    ???
  }

  it should "upload bytes to a resumable upload in multiple chunks" in {
    val bytesPerRequest = bodyTextNumberOfBytes / numberOfChunks

    val ranges = new ArrayBuffer[String]()

    val api = new GcsApi(req => {
      val checks = req.body.compile.toChunk.map { chunk =>
        req.method shouldBe Method.PUT
        req.uri shouldBe uploadURI
        req.headers.toList should contain allElementsOf List(
          `Content-Length`.unsafeFromLong(chunk.size.toLong)
        )
      }

      req.headers.get("Content-Range".ci) foreach { header =>
        ranges += header.value
      }

      Resource.liftF(checks).map { _ =>
        Response[IO](
          status = Status.Ok,
          headers = Headers.of(
            Header("Range", s"bytes 0-${bytesPerRequest - 1}/*")
          )
        )
      }
    })

    api
      .uploadBytes(
        bucket,
        uploadToken,
        0,
        bodyStream,
        bytesPerRequest
      )
      .unsafeRunSync() shouldBe Right(())

    ranges shouldBe (0 to numberOfChunks).map { index =>
      val chunkStart = index * bytesPerRequest
      if (index < numberOfChunks) {
        s"bytes $chunkStart-${chunkStart + bytesPerRequest - 1}/*"
      } else {
        s"bytes $chunkStart-${chunkStart + (bodyTextNumberOfBytes % bytesPerRequest) - 1}/*"
      }
    }
  }

  it should "upload bytes to a resumable upload in multiple chunks, without finishing" in {
    ???
  }

  it should "resend bytes that aren't recorded by GCS, and emit the last uploaded position" in {
    val bytes = Stream.random[IO].take(100).map(_.toByte)
    val ranges = new ArrayBuffer[(Long, Long)]()

    val api = new GcsApi(req => {
      val checks = req.body.compile.toChunk.map { chunk =>
        req.method shouldBe Method.PUT
        req.uri shouldBe uploadURI
        req.headers.toList should contain(
          `Content-Length`.unsafeFromLong(chunk.size.toLong)
        )
      }

      val getRange = for {
        _ <- checks
        header <- req.headers
          .get(`Content-Range`)
          .liftTo[IO](new RuntimeException)
        max <- header.range.second.liftTo[IO](new RuntimeException)
      } yield {
        (header.range.first, max)
      }

      Resource.liftF(getRange).map {
        case (min, max) =>
          ranges += (min -> max)

          val recordedBytes = max * 9 / 10
          val done = recordedBytes <= min
          Response[IO](
            status = Status(308),
            headers = Headers.of(Range(0, if (done) max else recordedBytes))
          )
      }
    })

    api
      .uploadChunk(bucket, uploadToken, 100, bytes, 100)
      .unsafeRunSync() shouldBe Left(200)

    ranges shouldBe List(100 -> 199, 180 -> 199)
  }

  it should "resend bytes that aren't recorded by GCS, finishing the upload" in {
    val bytes = Stream.random[IO].take(100).map(_.toByte)
    val ranges = new ArrayBuffer[(Long, Long)]()

    val api = new GcsApi(req => {
      val checks = req.body.compile.toChunk.map { chunk =>
        req.method shouldBe Method.PUT
        req.uri shouldBe uploadURI
        req.headers.toList should contain(
          `Content-Length`.unsafeFromLong(chunk.size.toLong)
        )
      }

      val getRange = for {
        _ <- checks
        header <- req.headers
          .get(`Content-Range`)
          .liftTo[IO](new RuntimeException)
        max <- header.range.second.liftTo[IO](new RuntimeException)
      } yield {
        (header.range.first, max)
      }

      Resource.liftF(getRange).map {
        case (min, max) =>
          ranges += (min -> max)

          val recordedBytes = max * 9 / 10
          val done = recordedBytes <= min
          Response[IO](
            status = if (done) Status.Ok else Status(308),
            headers = Headers.of(Range(0, if (done) max else recordedBytes))
          )
      }
    })

    api
      .uploadChunk(bucket, uploadToken, 50, bytes, 50)
      .unsafeRunSync() shouldBe Right(())

    ranges shouldBe List(
      50 -> 99,
      90 -> 139,
      126 -> 149,
      135 -> 149
    )
  }
}
