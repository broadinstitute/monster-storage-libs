package org.broadinstitute.monster.storage.gcs

import cats.effect.{IO, Resource}
import org.apache.commons.codec.digest.DigestUtils
import cats.implicits._
import fs2.{Chunk, Stream}
import org.http4s._
import org.http4s.headers._
import org.http4s.multipart.Multipart
import org.scalatest.{EitherValues, FlatSpec, Matchers, OptionValues}

import scala.collection.mutable.ArrayBuffer

class GcsApiSpec extends FlatSpec with Matchers with OptionValues with EitherValues {
  import GcsApi._

  private val bucket = "bucket"
  private val path = "the/path"
  private val uploadToken = "upload-token"
  private val smallChunkSize = 16

  private val readObjectURI = baseGcsUri(bucket, path, "alt" -> "media")
  private val statObjectURI = baseGcsUri(bucket, path, "alt" -> "json")
  private val createObjectOneShotURI = baseGcsUploadUri(bucket, "multipart")
  private val initResumableUploadURI = baseGcsUploadUri(bucket, "resumable")
  private val uploadURI =
    baseGcsUploadUri(bucket, "resumable").withQueryParam("upload_id", uploadToken)

  private val acceptEncodingHeader = Header("Accept-Encoding", "identity, gzip")

  private def bodyText(n: Int): Stream[IO, Byte] =
    Stream
      .randomSeeded(n.toLong)[IO]
      .map(String.valueOf)
      .flatMap(s => Stream.emits(s.getBytes))
      .take(n.toLong)
  private def stringify(bytes: Stream[IO, Byte]): IO[String] =
    bytes.through(fs2.text.utf8Decode).compile.toChunk.map(_.toArray[String].mkString(""))

  private def assertInitResumableUploadRequest(
    request: Request[IO],
    contentLength: Long,
    expectedSize: Long,
    chunk: Chunk[Byte],
    expectedMd5: Option[String]
  ): Unit = {
    request.method shouldBe Method.POST
    request.uri shouldBe initResumableUploadURI
    request.headers.toList should contain theSameElementsAs List(
      `Content-Length`.unsafeFromLong(contentLength),
      `Content-Type`(MediaType.application.json, Charset.`UTF-8`),
      Header(UploadContentLengthHeader, expectedSize.toString()),
      Header(UploadContentTypeHeader, "text/event-stream")
    )

    io.circe.parser
      .parse(new String(chunk.toArray[Byte]))
      .right
      .value shouldBe buildUploadMetadata(path, expectedMd5)
    ()
  }

  private def assertInitResumableUploadResponse(
    checks: IO[Unit],
    uploadToken: String
  ): Resource[IO, Response[IO]] = {
    Resource.liftF(checks).map { _ =>
      Response[IO](
        status = Status.Ok,
        headers = Headers.of(
          Header(UploadIDHeader, uploadToken)
        )
      )
    }
  }

  private def assertUploadBytesRequest(
    request: Request[IO],
    contentLength: Long
  ): Unit = {
    request.method shouldBe Method.PUT
    request.uri shouldBe uploadURI
    request.headers.toList should contain(
      `Content-Length`.unsafeFromLong(contentLength)
    )
    ()
  }

  private def assertUploadBytesResponse(
    checks: IO[Unit],
    request: Request[IO],
    ranges: ArrayBuffer[(Long, Long)],
    progressChunkSize: Long,
    lastByte: Long,
    allBytes: Boolean
  ): Resource[IO, Response[IO]] = {
    val getRange = for {
      _ <- checks
      header <- request.headers
        .get(`Content-Range`)
        .liftTo[IO](new RuntimeException)
      max <- header.range.second.liftTo[IO](new RuntimeException)
    } yield {
      (header.range.first, max)
    }

    Resource.liftF(getRange).map {
      case (min, max) =>
        ranges += (min -> max)

        val lastRecordedByte = math.min(max, min + progressChunkSize - 1)
        val done = lastRecordedByte == lastByte
        Response[IO](
          status = if (allBytes && done) Status.Ok else ResumeIncompleteStatus,
          headers = Headers.of(Range(0, lastRecordedByte))
        )
    }
  }

  behavior of "GcsApi"

  // readObject
  def testReadObject(
    description: String,
    numBytes: Int = smallChunkSize,
    start: Long = 0,
    end: Option[Long] = None,
    gzip: Boolean = false,
    forceGunzip: Boolean = false
  ): Unit = {
    it should description in {
      val baseBytes = bodyText(numBytes)
      val body = if (gzip) {
        baseBytes.through(fs2.compress.gzip(ChunkSize))
      } else {
        baseBytes
      }

      val api = new GcsApi(req => {
        req.method shouldBe Method.GET

        if (req.uri == statObjectURI) {
          Resource.liftF(body.compile.toChunk).map { chunk =>
            val metadata = if (gzip) {
              s"""{ "$ObjectSizeKey": ${chunk.size}, "$ObjectEncodingKey": "gzip" }"""
            } else {
              s"""{ "$ObjectSizeKey": ${chunk.size} }"""
            }
            Response[IO](body = Stream.emits(metadata.getBytes))
          }
        } else if (req.uri == readObjectURI) {
          req.headers.toList should contain(acceptEncodingHeader)

          val getRange = for {
            header <- req.headers.get(Range)
            end <- header.ranges.head.second
          } yield {
            (header.ranges.head.first, end)
          }

          getRange match {
            case None =>
              Resource.liftF(IO.raiseError(new Exception("Got request without range")))
            case Some((start, end)) =>
              val slice = body.drop(start).take(end - start + 1)
              Resource.pure(Response[IO](body = slice))
          }
        } else {
          Resource.liftF(IO.raiseError(new Exception(s"Saw unexpected URI: ${req.uri}")))
        }
      })

      val actual = stringify {
        api.readObjectByChunks(
          bucket,
          path,
          fromByte = start,
          untilByte = end,
          chunkSize = smallChunkSize,
          gunzipIfNeeded = gzip || forceGunzip
        )
      }
      val expected = stringify {
        val base = if (gzip) {
          body.through(fs2.compress.gunzip(ChunkSize))
        } else {
          body
        }.drop(start)

        end.fold(base)(n => base.take(n - start + 1))
      }

      actual.unsafeRunSync() shouldBe expected.unsafeRunSync()
    }
  }

  it should behave like testReadObject("read entire objects as a stream")

  it should behave like testReadObject(
    "read objects as a stream, using multiple requests",
    numBytes = (smallChunkSize * 2.5).toInt
  )

  it should behave like testReadObject(
    "gunzip compressed data if told to",
    gzip = true
  )

  it should behave like testReadObject(
    "not gunzip uncompressed data",
    forceGunzip = true
  )

  it should behave like testReadObject(
    "read objects starting at an offset",
    start = 100L
  )

  it should behave like testReadObject(
    "read objects ending before the final byte",
    end = Some(1000L)
  )

  it should behave like testReadObject(
    "read slices in the middle of an object",
    start = 100L,
    end = Some(1000L)
  )

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
    val theMd5 = "abcdefg"

    val api = new GcsApi(req => {
      req.method shouldBe Method.GET
      req.uri shouldBe statObjectURI
      Resource.pure(
        Response[IO](
          body = Stream.emits(s"""{"$ObjectMd5Key": "$theMd5"}""".getBytes())
        )
      )
    })

    api
      .statObject(bucket, path)
      .unsafeRunSync() shouldBe (true -> Some(theMd5))
  }

  private def singleShotApiRequest(stringBody: String, md5: Option[String]): GcsApi = {
    new GcsApi(req => {
      req.method shouldBe Method.POST
      req.uri shouldBe createObjectOneShotURI
      req.contentType.value.mediaType.isMultipart shouldBe true

      val dataChecks = req.as[Multipart[IO]].flatMap {
        multipart =>
          multipart.parts.size shouldBe 2
          val metadataPart = multipart.parts(0)
          val dataPart = multipart.parts(1)

          metadataPart.headers
            .get(`Content-Type`)
            .value
            .mediaType shouldBe MediaType.application.json
          dataPart.headers
            .get(`Content-Type`)
            .value shouldBe `Content-Type`(MediaType.`text/event-stream`)

          metadataPart.body.compile.toChunk.flatMap { metadataBytes =>
            io.circe.parser
              .parse(new String(metadataBytes.toArray[Byte]))
              .right
              .value shouldBe buildUploadMetadata(path, md5)

            dataPart.body.compile.toChunk.map { dataBytes =>
              new String(dataBytes.toArray[Byte]) shouldBe stringBody
            }
          }
      }

      Resource.liftF(dataChecks).map { _ =>
        Response[IO](status = Status.Ok)
      }
    })
  }

  // createObject
  it should "create a new object in GCS by using a multipart upload in a single shot" in {
    val bodyTextSize = MaxBytesPerUploadRequest / 2
    val baseBytes = bodyText(bodyTextSize)
    val doChecks = for {
      stringBody <- stringify(baseBytes)
      md5 = Some(DigestUtils.md5Hex(stringBody))
      api = singleShotApiRequest(stringBody, md5)
      _ <- api
        .createObject(
          bucket,
          path,
          `Content-Type`(MediaType.`text/event-stream`),
          bodyTextSize.toLong,
          md5,
          baseBytes
        )
    } yield ()

    doChecks.unsafeRunSync()
  }

  it should "create a new object in GCS by uploading in chunks" in {
    val ranges = new ArrayBuffer[(Long, Long)]()
    val bodyTextSize = MaxBytesPerUploadRequest * 2
    val baseBytes = bodyText(bodyTextSize)

    val doChecks = for {
      stringBody <- stringify(baseBytes)
      md5 = Some(DigestUtils.md5Hex(stringBody))

      api = new GcsApi(req => {
        val checks = req.body.compile.toChunk.map { bodyChunk =>
          if (req.uri == initResumableUploadURI) {
            assertInitResumableUploadRequest(
              req,
              bodyChunk.size.toLong,
              bodyTextSize.toLong,
              bodyChunk,
              md5
            )
          } else {
            assertUploadBytesRequest(req, bodyChunk.size.toLong)
          }
        }
        if (req.uri == initResumableUploadURI) {
          assertInitResumableUploadResponse(checks, uploadToken)
        } else {
          assertUploadBytesResponse(
            checks,
            req,
            ranges,
            MaxBytesPerUploadRequest * 2L,
            bodyTextSize - 1L,
            true
          )
        }
      })

      _ <- api
        .createObject(
          bucket,
          path,
          `Content-Type`(MediaType.`text/event-stream`),
          bodyTextSize.toLong,
          md5,
          baseBytes
        )
    } yield ()

    doChecks.unsafeRunSync()
    ranges shouldBe (0 to bodyTextSize - 1 by ChunkSize).map { n =>
      n -> math.min(bodyTextSize - 1, n + ChunkSize - 1)
    }
  }

  // createObjectOneShot
  def testCreateObjectOneShot(description: String, includeMd5: Boolean): Unit = {
    it should description in {
      val baseBytes = bodyText(smallChunkSize)

      val doChecks = for {
        stringBody <- stringify(baseBytes)
        md5 = if (includeMd5) Some(DigestUtils.md5Hex(stringBody)) else None
        api = singleShotApiRequest(stringBody, md5)
        _ <- api
          .createObjectOneShot(
            bucket,
            path,
            `Content-Type`(MediaType.`text/event-stream`),
            md5,
            baseBytes
          )
      } yield ()

      doChecks.unsafeRunSync()
    }
  }

  it should behave like testCreateObjectOneShot(
    "create a GCS object using a multipart upload",
    includeMd5 = false
  )
  it should behave like testCreateObjectOneShot(
    "include expected md5s in multipart upload requests",
    includeMd5 = true
  )

  // deleteObject
  def testDeleteObject(description: String, exists: Boolean): Unit = {
    it should description in {
      val api = new GcsApi(req => {
        req.method shouldBe Method.DELETE
        req.uri shouldBe baseGcsUri(bucket, path)
        Resource.pure(Response[IO](status = if (exists) Status.Ok else Status.NotFound))
      })

      api.deleteObject(bucket, path).unsafeRunSync() shouldBe exists
    }
  }

  it should behave like testDeleteObject(
    "delete a GCS object and return true",
    exists = true
  )

  it should behave like testDeleteObject(
    "no-op when deleting a GCS object that doesn't exist, and return false",
    exists = false
  )

  // initResumableUpload
  def testInitUpload(description: String, withMd5: Boolean): Unit = {
    it should description in {
      val expectedSize = 10L
      val expectedMd5 = if (withMd5) Some("abcdef") else None

      val api = new GcsApi(req => {
        val checks = req.body.compile.toChunk.map { bodyChunk =>
          assertInitResumableUploadRequest(
            req,
            bodyChunk.size.toLong,
            expectedSize,
            bodyChunk,
            expectedMd5
          )
        }

        assertInitResumableUploadResponse(checks, uploadToken)
      })

      api
        .initResumableUpload(
          bucket,
          path,
          `Content-Type`(MediaType.`text/event-stream`),
          expectedSize,
          expectedMd5
        )
        .unsafeRunSync() shouldBe uploadToken
    }
  }

  it should behave like testInitUpload(
    "initialize a resumable upload with no md5",
    withMd5 = false
  )

  it should behave like testInitUpload(
    "initialize a resumable upload with an md5",
    withMd5 = true
  )

  // uploadBytes
  def testUpload(
    description: String,
    allBytes: Boolean,
    numBytes: Int = smallChunkSize,
    start: Long = 0L,
    recordedRatio: Double = 1.0
  ): Unit = {
    it should description in {
      val ranges = new ArrayBuffer[(Long, Long)]()

      val progressChunkSize = (smallChunkSize * recordedRatio).toInt
      val lastByte = start + numBytes - 1
      val bytes = bodyText(numBytes)

      val api = new GcsApi(req => {
        val checks = req.body.compile.toChunk.map { chunk =>
          assertUploadBytesRequest(req, chunk.size.toLong)
        }

        assertUploadBytesResponse(
          checks,
          req,
          ranges,
          progressChunkSize.toLong,
          lastByte,
          allBytes
        )
      })

      val actual = api
        .uploadByteChunks(bucket, uploadToken, start, smallChunkSize, bytes)
        .unsafeRunSync()
      val expected = if (allBytes) Right(()) else Left(start + numBytes)

      actual shouldBe expected
      ranges shouldBe (start to lastByte by progressChunkSize.toLong).map { n =>
        n -> math.min(lastByte, n + smallChunkSize - 1)
      }
    }
  }

  it should behave like testUpload(
    "upload bytes to a resumable upload in a single chunk",
    allBytes = true
  )

  it should behave like testUpload(
    "upload bytes to a resumable upload for a single chunk, without finishing",
    allBytes = false
  )

  it should behave like testUpload(
    "upload bytes to a resumable upload in multiple chunks",
    allBytes = true,
    numBytes = (smallChunkSize * 2.5).toInt
  )

  it should behave like testUpload(
    "upload bytes to a resumable upload in multiple chunks, without finishing",
    allBytes = false,
    numBytes = (smallChunkSize * 2.5).toInt
  )

  it should behave like testUpload(
    "upload bytes to a resumable upload from an offset in the file",
    allBytes = false,
    start = 128L
  )

  it should behave like testUpload(
    "resend bytes that aren't recorded by GCS, and emit the last uploaded position",
    allBytes = false,
    recordedRatio = 0.9
  )

  it should behave like testUpload(
    "resend bytes that aren't recorded by GCS, finishing the upload",
    allBytes = true,
    recordedRatio = 0.35
  )

  it should behave like testUpload(
    "upload bytes from an offset in the file, resending bytes that aren't recorded by GCS",
    allBytes = true,
    start = 128L,
    recordedRatio = 0.5
  )
}
