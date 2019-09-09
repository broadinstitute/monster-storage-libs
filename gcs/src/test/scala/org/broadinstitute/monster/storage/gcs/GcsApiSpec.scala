package org.broadinstitute.monster.storage.gcs

import cats.effect.{IO, Resource}
import org.apache.commons.codec.digest.DigestUtils
import cats.implicits._
import fs2.Stream
import io.circe.{Json, JsonObject}
import io.circe.syntax._
import org.broadinstitute.monster.storage.common.FileType
import org.http4s._
import org.http4s.headers._
import org.http4s.multipart.Multipart
import org.scalatest.{EitherValues, FlatSpec, Matchers, OptionValues}

import scala.collection.mutable.ArrayBuffer

class GcsApiSpec extends FlatSpec with Matchers with OptionValues with EitherValues {
  import GcsApi._

  private val bucket = "bucket"
  private val bucket2 = "bucket2"
  private val path = "the/path"
  private val path2 = "the/path2"
  private val uploadToken = "upload-token"
  private val listToken = "list-token"
  private val copyToken = "copy-token"
  private val smallChunkSize = 16
  private val smallPageSize = 16

  private val objectURI = objectUri(bucket, path)
  private val readObjectURI = objectDataUri(bucket, path)
  private val statObjectURI = objectMetadataUri(bucket, path)
  private val createObjectOneShotURI = multipartUploadUri(bucket)
  private val initResumableUploadURI = resumableUploadUri(bucket, None)
  private val uploadURI = resumableUploadUri(bucket, Some(uploadToken))
  private val listURI = listUri(bucket, path, smallPageSize, None)
  private val continueListURI = listUri(bucket, path, smallPageSize, Some(listToken))
  private val copyURI = rewriteUri(bucket, path, bucket2, path2, None)
  private val continueCopyURI = rewriteUri(bucket, path, bucket2, path2, Some(copyToken))

  private val acceptEncodingHeader = Header("Accept-Encoding", "identity, gzip")

  private def bodyText(n: Long): Stream[IO, Byte] =
    Stream
      .randomSeeded(n)[IO]
      .map(String.valueOf)
      .flatMap(s => Stream.emits(s.getBytes))
      .take(n)
  private def stringify(bytes: Stream[IO, Byte]): IO[String] =
    bytes.through(fs2.text.utf8Decode).compile.toChunk.map(_.toArray[String].mkString(""))

  private def assertInitResumableUpload(
    request: Request[IO],
    expectedSize: Long,
    expectedMd5: Option[String],
    uploadToken: String
  ): Resource[IO, Response[IO]] = {
    val checks = request.body.compile.toChunk.map { bodyChunk =>
      request.method shouldBe Method.POST
      request.uri shouldBe initResumableUploadURI
      request.headers.toList should contain theSameElementsAs List(
        `Content-Length`.unsafeFromLong(bodyChunk.size.toLong),
        `Content-Type`(MediaType.application.json, Charset.`UTF-8`),
        Header(UploadContentLengthHeader, expectedSize.toString),
        Header(UploadContentTypeHeader, "text/event-stream")
      )

      io.circe.parser
        .parse(new String(bodyChunk.toArray[Byte]))
        .right
        .value shouldBe buildUploadMetadata(path, expectedMd5)
      ()
    }

    Resource.liftF(checks).map { _ =>
      Response[IO](
        status = Status.Ok,
        headers = Headers.of(
          Header(UploadIDHeader, uploadToken)
        )
      )
    }
  }

  private def assertUploadBytes(
    request: Request[IO],
    ranges: ArrayBuffer[(Long, Long)],
    progressChunkSize: Long,
    lastByte: Long,
    allBytes: Boolean
  ): Resource[IO, Response[IO]] = {
    val checks = request.body.compile.toChunk.map { bodyChunk =>
      request.method shouldBe Method.PUT
      request.uri shouldBe uploadURI
      request.headers.toList should contain(
        `Content-Length`.unsafeFromLong(bodyChunk.size.toLong)
      )
    }

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
    numBytes: Long = smallChunkSize.toLong,
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
    numBytes = (smallChunkSize * 2.5).toLong
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
          bodyTextSize,
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
        if (req.uri == initResumableUploadURI) {
          assertInitResumableUpload(
            req,
            bodyTextSize,
            md5,
            uploadToken
          )
        } else {
          assertUploadBytes(
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
          bodyTextSize,
          md5,
          baseBytes
        )
    } yield ()

    doChecks.unsafeRunSync()
    ranges shouldBe (0 until bodyTextSize.toInt by ChunkSize).map { n =>
      n -> math.min(bodyTextSize.toInt - 1, n + ChunkSize - 1)
    }
  }

  // createObjectOneShot
  def testCreateObjectOneShot(description: String, includeMd5: Boolean): Unit = {
    it should description in {
      val baseBytes = bodyText(smallChunkSize.toLong)

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
        req.uri shouldBe objectURI
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
        assertInitResumableUpload(req, expectedSize, expectedMd5, uploadToken)
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
    numBytes: Long = smallChunkSize.toLong,
    start: Long = 0L,
    recordedRatio: Double = 1.0
  ): Unit = {
    it should description in {
      val ranges = new ArrayBuffer[(Long, Long)]()

      val progressChunkSize = (smallChunkSize * recordedRatio).toInt
      val lastByte = start + numBytes - 1
      val bytes = bodyText(numBytes)

      val api = new GcsApi(req => {
        assertUploadBytes(
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
    numBytes = (smallChunkSize * 2.5).toLong
  )

  it should behave like testUpload(
    "upload bytes to a resumable upload in multiple chunks, without finishing",
    allBytes = false,
    numBytes = (smallChunkSize * 2.5).toLong
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

  private def buildListResponse(
    prefixes: Option[List[String]],
    files: Option[List[String]],
    nextToken: Option[String]
  ): Json = {
    val base = JsonObject.empty
    val withToken = nextToken.fold(base)(t => base.add(GcsApi.ListTokenKey, t.asJson))
    val withPrefixes =
      prefixes.fold(withToken)(ps => withToken.add(GcsApi.ListPrefixesKey, ps.asJson))
    val withFiles = files.fold(withPrefixes) { fs =>
      withPrefixes.add(
        GcsApi.ListResultsKey,
        fs.map(f => Json.obj(GcsApi.ObjectNameKey -> f.asJson)).asJson
      )
    }
    withFiles.asJson
  }

  it should "list directory contents" in {
    val expectedPrefixes = List("foo/", "bar/")
    val expectedFiles = List("file1", "file2")

    val api = new GcsApi(req => {
      req.method shouldBe Method.GET
      req.uri shouldBe listURI

      val body =
        buildListResponse(Some(expectedPrefixes), Some(expectedFiles), None).noSpaces
      Resource.pure(Response[IO](status = Status.Ok, body = Stream.emits(body.getBytes)))
    })

    val results =
      api.listContents(bucket, path, smallPageSize).compile.toList.unsafeRunSync()
    val expected = expectedPrefixes.map(_ -> FileType.Directory) :::
      expectedFiles.map(_ -> FileType.File)
    results should contain theSameElementsAs expected
  }

  it should "query all pages of list results" in {
    val expectedPrefixes = List(List("foo/"), List("bar/"), List("baz/"))
    val expectedFiles = List(List("file1"), List("file2"), List("file3"))
    var pagesPulled = 0

    val api = new GcsApi(req => {
      req.method shouldBe Method.GET
      if (pagesPulled == 0) {
        req.uri shouldBe listURI
      } else {
        req.uri shouldBe continueListURI
      }

      val body = buildListResponse(
        Some(expectedPrefixes(pagesPulled)),
        Some(expectedFiles(pagesPulled)),
        Some(listToken).filter(_ => pagesPulled + 1 < expectedFiles.length)
      ).noSpaces
      pagesPulled += 1
      Resource.pure(Response[IO](status = Status.Ok, body = Stream.emits(body.getBytes)))
    })

    val results =
      api.listContents(bucket, path, smallPageSize).compile.toList.unsafeRunSync()
    val expected = expectedPrefixes.flatten.map(_ -> FileType.Directory) :::
      expectedFiles.flatten.map(_ -> FileType.File)
    results should contain theSameElementsAs expected
  }

  it should "not break if GCS returns no prefixes in list results" in {
    val expectedFiles = List("file1", "file2")

    val api = new GcsApi(req => {
      req.method shouldBe Method.GET
      req.uri shouldBe listURI

      val body = buildListResponse(None, Some(expectedFiles), None).noSpaces
      Resource.pure(Response[IO](status = Status.Ok, body = Stream.emits(body.getBytes)))
    })

    val results =
      api.listContents(bucket, path, smallPageSize).compile.toList.unsafeRunSync()
    val expected = expectedFiles.map(_ -> FileType.File)
    results should contain theSameElementsAs expected
  }

  it should "not break if GCS returns only prefixes in list results" in {
    val expectedPrefixes = List("foo/", "bar/")

    val api = new GcsApi(req => {
      req.method shouldBe Method.GET
      req.uri shouldBe listURI

      val body = buildListResponse(Some(expectedPrefixes), None, None).noSpaces
      Resource.pure(Response[IO](status = Status.Ok, body = Stream.emits(body.getBytes)))
    })

    val results =
      api.listContents(bucket, path, smallPageSize).compile.toList.unsafeRunSync()
    val expected = expectedPrefixes.map(_ -> FileType.Directory)
    results should contain theSameElementsAs expected
  }

  it should "raise a helpful error if GCS returns no list results" in {
    val api = new GcsApi(req => {
      req.method shouldBe Method.GET
      req.uri shouldBe listURI

      val body = buildListResponse(None, None, None).noSpaces
      Resource.pure(Response[IO](status = Status.Ok, body = Stream.emits(body.getBytes)))
    })

    val resultsOrErr =
      api.listContents(bucket, path, smallPageSize).compile.drain.attempt.unsafeRunSync()
    resultsOrErr.left.value.getMessage should include(path)
  }

  it should "raise a helpful error if GCS returns an error code to a list request" in {
    val body = "OH NO"
    val api = new GcsApi(req => {
      req.method shouldBe Method.GET
      req.uri shouldBe listURI

      Resource.pure(
        Response[IO](
          status = Status.InternalServerError,
          body = Stream.emits(body.getBytes)
        )
      )
    })

    val resultsOrErr =
      api.listContents(bucket, path, smallPageSize).compile.drain.attempt.unsafeRunSync()
    resultsOrErr.left.value.getMessage should include(path)
    resultsOrErr.left.value.getMessage should include(body)
  }

  it should "copy objects in GCS in one shot" in {
    val api = new GcsApi(req => {
      req.method shouldBe Method.POST
      req.uri shouldBe copyURI

      Resource.pure(Response[IO](status = Status.Ok, body = Stream.emits("{}".getBytes)))
    })

    api.copyObject(bucket, path, bucket2, path2).unsafeRunSync()
  }

  it should "copy objects in GCS through multiple requests" in {
    var requests = 0
    val api = new GcsApi(req => {
      req.method shouldBe Method.POST
      if (requests == 0) {
        req.uri shouldBe copyURI
      } else {
        req.uri shouldBe continueCopyURI
      }
      requests += 1

      val response = if (requests < 5) {
        Json.obj(CopyTokenKey -> copyToken.asJson)
      } else {
        Json.obj()
      }
      Resource.pure(
        Response[IO](status = Status.Ok, body = Stream.emits(response.noSpaces.getBytes))
      )
    })

    api.copyObject(bucket, path, bucket2, path2).unsafeRunSync()
    requests shouldBe 5
  }

  it should "initialize long-running internal GCS copy operations" in {
    val api = new GcsApi(req => {
      req.method shouldBe Method.POST
      req.uri shouldBe copyURI

      val response = Json.obj(CopyTokenKey -> copyToken.asJson).noSpaces
      Resource.pure(
        Response[IO](status = Status.Ok, body = Stream.emits(response.getBytes))
      )
    })

    api
      .initializeCopy(bucket, path, bucket2, path2)
      .unsafeRunSync() shouldBe Left(copyToken)
  }

  it should "continue long-running internal GCS copy operations" in {
    val api = new GcsApi(req => {
      req.method shouldBe Method.POST
      req.uri shouldBe continueCopyURI

      val response = Json.obj(CopyTokenKey -> copyToken.asJson).noSpaces
      Resource.pure(
        Response[IO](status = Status.Ok, body = Stream.emits(response.getBytes))
      )
    })

    api
      .incrementCopy(bucket, path, bucket2, path2, copyToken)
      .unsafeRunSync() shouldBe Left(copyToken)
  }

  it should "finish long-running internal GCS copy operations" in {
    val api = new GcsApi(req => {
      req.method shouldBe Method.POST
      req.uri shouldBe continueCopyURI

      Resource.pure(
        Response[IO](status = Status.Ok, body = Stream.emits("{}".getBytes))
      )
    })

    api
      .incrementCopy(bucket, path, bucket2, path2, copyToken)
      .unsafeRunSync() shouldBe Right(())
  }

  it should "raise a helpful error when copy operations return an error code" in {
    val error = "OH NO"
    val api = new GcsApi(_ => {
      Resource.pure(
        Response[IO](
          status = Status.InternalServerError,
          body = Stream.emits(error.getBytes)
        )
      )
    })

    val result = api.copyObject(bucket, path, bucket2, path2).attempt.unsafeRunSync()
    val message = result.left.value.getMessage
    message should include(bucket)
    message should include(path)
    message should include(bucket2)
    message should include(path2)
    message should include(error)
  }
}
