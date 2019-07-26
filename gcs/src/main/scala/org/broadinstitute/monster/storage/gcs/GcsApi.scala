package org.broadinstitute.monster.storage.gcs

import java.nio.file.Path

import cats.data.NonEmptyList
import cats.effect.{Clock, ContextShift, IO, Resource}
import cats.implicits._
import fs2.{Chunk, Pull, Stream}
import io.circe.jawn.JawnParser
import io.circe.syntax._
import io.circe.{Json, JsonObject}
import org.apache.commons.codec.binary.{Base64, Hex}
import org.http4s._
import org.http4s.client.Client
import org.http4s.headers._
import org.http4s.multipart._
import org.http4s.util.CaseInsensitiveString

/**
  * Client which can perform I/O operations against Google Cloud Storage.
  *
  * @param runHttp function which can transform HTTP requests into HTTP responses
  *                (bracketed by connection-management code)
  */
class GcsApi private[gcs] (runHttp: Request[IO] => Resource[IO, Response[IO]]) {
  import GcsApi._

  private val parser = new JawnParser()
  private val applicationJsonContentType =
    `Content-Type`(MediaType.application.json, Charset.`UTF-8`)

  private def getObjectMetadata(
    bucket: String,
    path: String
  ): Resource[IO, Response[IO]] = runHttp {
    Request[IO](method = Method.GET, uri = baseGcsUri(bucket, path, "alt" -> "json"))
  }

  /**
    * Read a range of bytes (potentially the whole file) from an object in cloud storage.
    *
    * @param bucket name of the GCS bucket to read from
    * @param path path within `bucket` containing the object-to-read
    * @param fromByte first byte (zero-indexed, inclusive) within the object at `path`
    *                 which should be included in the response from GCS
    * @param untilByte exclusive endpoint for the bytes returned from the object at `path`,
    *                  or `None` if all bytes should be returned
    */
  def readObject(
    bucket: String,
    path: String,
    fromByte: Long = 0L,
    untilByte: Option[Long] = None,
    gunzipIfNeeded: Boolean = false
  ): Stream[IO, Byte] =
    readObjectByChunks(bucket, path, fromByte, untilByte, ChunkSize, gunzipIfNeeded)

  /**
    * Read a range of bytes (potentially the whole file) from an object in cloud storage.
    *
    * This is a helper method for the public-facing `readObject`, which allows for setting
    * a custom chunk size. We expose it within the package so unit tests don't have to
    * generate huge streams of test data.
    */
  private[gcs] def readObjectByChunks(
    bucket: String,
    path: String,
    fromByte: Long,
    untilByte: Option[Long],
    chunkSize: Int,
    gunzipIfNeeded: Boolean
  ): Stream[IO, Byte] = {
    val objectUri = baseGcsUri(bucket, path, "alt" -> "media")

    def pullBytes(startByte: Long, endByte: Long): Stream[IO, Byte] = {
      val finalRequest = endByte - startByte - 1 <= chunkSize
      val rangeEnd = if (finalRequest) endByte else startByte + chunkSize
      val range = Range(startByte, rangeEnd - 1)
      // Tell GCS that we're OK accepting gzipped data to prevent it from trying to
      // decompress on the server-side, because that breaks use of the 'Range' header.
      val accept =
        `Accept-Encoding`(NonEmptyList.of(ContentCoding.identity, ContentCoding.gzip))

      val request = Request[IO](
        method = Method.GET,
        uri = objectUri,
        headers = Headers.of(range, accept)
      )

      val responseStream = Stream.resource(runHttp(request)).flatMap { response =>
        if (response.status.isSuccess) {
          response.body
        } else {
          Stream.eval(
            reportError(response, s"Failed to get object bytes from $path in $bucket")
          )
        }
      }

      if (finalRequest) {
        responseStream
      } else {
        responseStream.append(pullBytes(rangeEnd, endByte))
      }
    }

    val getObjectInfo = getObjectMetadata(bucket, path).use { response =>
      if (response.status.isSuccess) {
        for {
          byteChunk <- response.body.compile.toChunk
          objectMetadata <- parser
            .parseByteBuffer(byteChunk.toByteBuffer)
            .flatMap(_.as[Json])
            .liftTo[IO]
          objectSize <- objectMetadata.hcursor.get[Long](ObjectSizeKey).liftTo[IO]
        } yield {
          val maybeEncoding = for {
            rawEncoding <- objectMetadata.hcursor.get[String](ObjectEncodingKey)
            parsedEncoding <- ContentCoding.fromString(rawEncoding)
          } yield {
            parsedEncoding
          }

          (objectSize, maybeEncoding.toOption)
        }
      } else {
        reportError(response, s"Failed to get object metadata from $path in $bucket")
      }
    }

    Stream.eval(getObjectInfo).flatMap {
      case (objectSize, encoding) =>
        val endByte = untilByte.getOrElse(objectSize)
        val rawBytes = pullBytes(fromByte, endByte)

        if (gunzipIfNeeded && encoding.contains(ContentCoding.gzip)) {
          rawBytes.through(fs2.compress.gunzip(chunkSize))
        } else {
          rawBytes
        }
    }
  }

  /**
    * Check if an object exists in GCS.
    *
    * @param bucket name of the GCS bucket to check within
    * @param path the path within `bucket` to check
    * @return a boolean indicating if an object exists at `path` in `bucket`, and the
    *         md5 of the object if Google calculated one during the upload
    */
  def statObject(bucket: String, path: String): IO[(Boolean, Option[String])] =
    getObjectMetadata(bucket, path).use { response =>
      if (response.status == Status.NotFound) {
        IO.pure((false, None))
      } else if (response.status.isSuccess) {
        for {
          byteChunk <- response.body.compile.toChunk
          objectMetadata <- parser
            .parseByteBuffer(byteChunk.toByteBuffer)
            .flatMap(_.as[JsonObject])
            .liftTo[IO]
        } yield {
          (true, objectMetadata(ObjectMd5Key).flatMap(_.asString))
        }
      } else {
        reportError(response, s"Failed to get object metadata from $path in $bucket")
      }
    }


  // TODO: if the data is greater than n bytes use initResumableUpload and the uploadBytes in multiple chunks
  /**
    * Create a new object in GCS using a multipart upload.
    *
    * One-shot uploads are recommended for any object less than 5MB. Multipart is the one
    * mechanism to do a one-shot upload while still setting object metadata.
    *
    * @see https://cloud.google.com/storage/docs/json_api/v1/how-tos/multipart-upload
    *
    * @param bucket the GCS bucket to create the new object within
    * @param path path within `bucket` where the new object will be created
    * @param contentType content-type to set on the new object
    * @param expectedMd5 expected md5, if any, of the new object. Setting this will enable
    *                    server-side content validation in GCS
    * @param data bytes to write into the new file
    */
  def createObject(
    bucket: String,
    path: String,
    contentType: `Content-Type`,
    expectedMd5: Option[String],
    data: Stream[IO, Byte]
  ): IO[Unit] = {
    val multipartBoundary = Boundary.create
    val objectMetadata = buildUploadMetadata(path, expectedMd5)

    val delimiter = s"--${multipartBoundary.value}"
    val end = s"$delimiter--"

    val dataHeader = List(
      delimiter,
      applicationJsonContentType.renderString,
      "",
      objectMetadata.noSpaces,
      "",
      delimiter,
      contentType.renderString,
      ""
    ).mkString("", Boundary.CRLF, Boundary.CRLF)

    val dataFooter = List("", end).mkString(Boundary.CRLF)

    val fullBody = Stream
      .emits(dataHeader.getBytes)
      .append(data)
      .append(Stream.emits(dataFooter.getBytes))

    fullBody.compile.toChunk.flatMap { chunk =>
      val fullHeaders = Headers.of(
        `Content-Type`(MediaType.multipartType("related", Some(multipartBoundary.value))),
        `Content-Length`.unsafeFromLong(chunk.size.toLong)
      )

      val gcsReq = Request[IO](
        method = Method.POST,
        uri = baseGcsUploadUri(bucket, "multipart"),
        headers = fullHeaders,
        body = Stream.chunk(chunk)
      )
      runHttp(gcsReq).use { response =>
        if (response.status.isSuccess) {
          IO.unit
        } else {
          reportError(response, s"Failed to create object for $path in $bucket")
        }
      }
    }
  }

  /**
    * Initialize a GCS resumable upload.
    *
    * Resumable uploads enable pushing data to an object in multiple chunks, and are recommended
    * for use with files larger than 5MB. The initialization request for the upload must include
    * any metadata for the object that will be created.
    *
    * @see https://cloud.google.com/storage/docs/json_api/v1/how-tos/resumable-upload#start-resumable
    *
    * @param bucket the GCS bucket to create the new object within
    * @param path path within `bucket` where the new object will be created
    * @param contentType content-type to set on the new object
    * @param expectedSize total number of bytes that will be uploaded to the new object over
    *                     the course of all subsequent requests
    * @param expectedMd5 expected md5, if any, of the new object. Setting this will enable
    *                    server-side content validation in GCS
    * @return the unique ID of this upload, for use in subsequent requests
    */
  def initResumableUpload(
    bucket: String,
    path: String,
    contentType: `Content-Type`,
    expectedSize: Long,
    expectedMd5: Option[String]
  ): IO[String] = {
    val objectMetadata = buildUploadMetadata(path, expectedMd5).noSpaces.getBytes

    val gcsReq = Request[IO](
      method = Method.POST,
      uri = baseGcsUploadUri(bucket, "resumable"),
      body = Stream.emits(objectMetadata),
      headers = Headers.of(
        `Content-Length`.unsafeFromLong(objectMetadata.length.toLong),
        applicationJsonContentType,
        Header(UploadContentLengthHeader, expectedSize.toString),
        contentType.toRaw.copy(
          name = CaseInsensitiveString(UploadContentTypeHeader)
        )
      )
    )

    runHttp(gcsReq).use { response =>
      response.headers
        .get(CaseInsensitiveString(UploadIDHeader))
        .map { _.value }
        .liftTo[IO](
          new RuntimeException(
            s"No upload token returned after initializing upload to $path in $bucket"
          )
        )
    }
  }

  /**
    * Upload bytes to an ongoing GCS resumable upload.
    *
    * @see https://cloud.google.com/storage/docs/json_api/v1/how-tos/resumable-upload#upload-resumable
    *
    * @param bucket the GCS bucket to upload the data into
    * @param uploadToken the unique ID of the previously-initialized resumable upload
    * @param rangeStart the (zero-indexed) position of the first byte of `data` within
    *                   the source file being uploaded
    * @param data bytes to upload
    * @return either the number of bytes stored by GCS so far (signalling that the upload is not complete),
    *         or Unit when all bytes have been received and GCS signals the upload is done
    */
  def uploadBytes(
    bucket: String,
    uploadToken: String,
    rangeStart: Long,
    data: Stream[IO, Byte]
  ): IO[Either[Long, Unit]] =
    uploadByteChunks(bucket, uploadToken, rangeStart, ChunkSize, data)

  /**
    * Write a stream of bytes (potentially the whole file) to a resumable upload in cloud storage.
    *
    * This is a helper method for the public-facing `uploadBytes`, which allows for setting
    * a custom chunk size. We expose it within the package so unit tests don't have to
    * generate huge streams of test data.
    */
  private[gcs] def uploadByteChunks(
    bucket: String,
    uploadToken: String,
    rangeStart: Long,
    chunkSize: Int,
    data: Stream[IO, Byte]
  ): IO[Either[Long, Unit]] = {

    // Helper method to incrementally push data from the input stream to GCS.
    def pushChunks(start: Long, data: Stream[IO, Byte]): Pull[IO, Nothing, Option[Long]] =
      // Pull the first chunk of data off the front of the stream.
      data.pull
        .unconsN(chunkSize, allowFewer = true)
        .flatMap {
          // If the stream was empty, mark that we haven't made any progress
          // from the initial byte.
          case None => Pull.pure(Some(start))

          // If a chunk of data was pulled, upload it to GCS.
          case Some((chunk, remaining)) =>
            val chunkSize = chunk.size
            val chunkEnd = start + chunkSize - 1

            val gcsReq = Request[IO](
              method = Method.PUT,
              uri = baseGcsUploadUri(bucket, "resumable")
                .withQueryParam("upload_id", uploadToken),
              headers = Headers.of(
                `Content-Length`.unsafeFromLong(chunkSize.toLong),
                `Content-Range`(start, chunkEnd)
              ),
              body = Stream.chunk(chunk)
            )

            Pull.eval {
              runHttp(gcsReq).use { response =>
                // GCS uses a redirect code to signal that more bytes should be uploaded.
                if (response.status == ResumeIncompleteStatus) {
                  val bytesReceived = for {
                    range <- response.headers.get(Range)
                    rangeEnd <- range.ranges.head.second
                  } yield {
                    rangeEnd
                  }
                  // If Google doesn't report a range, the safest thing to do is retry.
                  // Otherwise convert the returned inclusive endpoint to an exclusive
                  // endpoint by adding 1.
                  IO.pure(Some(bytesReceived.fold(start)(_ + 1)))
                } else if (response.status.isSuccess) {
                  IO.pure(None)
                } else {
                  reportError(
                    response,
                    s"Failed to upload chunk with upload token: $uploadToken"
                  )
                }
              }
            }.flatMap {
              // If there are more bytes to upload, try again recursively.
              case Some(nextByte) =>
                val numBytesUploadedFromChunk = nextByte - start
                pushChunks(
                  nextByte,
                  // Make sure we don't double-upload bytes that GCS did
                  // manage to store.
                  Stream.chunk(chunk).drop(numBytesUploadedFromChunk).append(remaining)
                )
              case None => Pull.pure(None)
            }
        }

    pushChunks(rangeStart, data)
      .flatMap(result => Pull.output(Chunk(result)))
      .stream
      .compile
      .lastOrError
      .map(_.toLeft(()))
  }

  /**
    * Delete an object in GCS if exists.
    *
    * @param bucket name of the bucket containing the object to delete
    * @param path path within `bucket` pointing to the object to delete
    * @return true if an object was actually deleted, otherwise false
    */
  def deleteObject(bucket: String, path: String): IO[Boolean] = {
    val gcsUri = baseGcsUri(bucket, path)
    val gcsReq = Request[IO](method = Method.DELETE, uri = gcsUri)

    runHttp(gcsReq).use { response =>
      if (response.status.isSuccess || response.status == Status.NotFound) {
        IO.pure(response.status.isSuccess)
      } else {
        reportError(response, s"Failed to delete object $path in $bucket")
      }
    }
  }
}

object GcsApi {

  /** Exception raised when GCS returns an error status. */
  case class GcsFailure(status: Status, body: String, message: String)
      extends Exception(
        s"""$message
           |Failed response returned $status:
           |$body""".stripMargin
      )

  /**
    * Convert a failed HTTP response into an error capturing info needed for debugging / retries.
    *
    * @param failedResponse the failed responses with a given status from an HTTP requests
    * @param additionalErrorMessage Any extra information the should be added to the raised error
    */
  private def reportError[A](
    failedResponse: Response[IO],
    additionalErrorMessage: String
  ): IO[A] =
    failedResponse.body.compile.toChunk.flatMap { chunk =>
      val error = GcsFailure(
        failedResponse.status,
        new String(chunk.toArray[Byte]),
        additionalErrorMessage
      )
      IO.raiseError(error)
    }

  /**
    * Construct a client which will send authorized requests to GCS over HTTP.
    *
    * All connection management / internal HTTP logic is handled by the client
    * passed as input to the builder, allowing users to inject arbitrary implementations
    * or middleware as needed.
    *
    * If a path to service account JSON is given, that account will be used to
    * build authorization headers for outgoing requests. Otherwise the environment's
    * application-default credentials will be used.
    *
    * @param httpClient client which will actually send HTTP requests / parse responses
    * @param serviceAccountJson path to service account credentials to use in place
    *                           of application default credentials
    */
  def build(
    httpClient: Client[IO],
    serviceAccountJson: Option[Path]
  )(implicit cs: ContextShift[IO], clk: Clock[IO]): IO[GcsApi] =
    GcsAuthProvider.build(serviceAccountJson).map { auth =>
      /*
       * NOTE: Injecting the call to `.addAuth` here instead of in the class body
       * has the following trade-off:
       *
       *   1. It prevents the auth-adding logic from being covered by unit tests, BUT
       *   2. It makes it impossible for the business logic in the class body to
       *      "forget" to add auth, so as long as we get the logic correct here we
       *      can be confident that HTTP calls in the client won't fail from that
       *      particular bug
       *
       * 2. is a huge win for code cleanliness and long-term maintenance. Our integration
       * tests will thoroughly cover this builder method, so 1. isn't that big of a problem.
       */
      new GcsApi(req => Resource.liftF(auth.addAuth(req)).flatMap(httpClient.run))
    }

  /** Build the JSON API endpoint for an existing bucket/path in GCS. */
  def baseGcsUri(bucket: String, path: String, queryParams: (String, String)*): Uri = {
    // NOTE: Calls to the `/` method cause the RHS to be URL-encoded.
    // This is the correct behavior in this case, but it can cause confusion.
    val uri = uri"https://www.googleapis.com/storage/v1/b/" / bucket / "o" / path
    uri.copy(query = Query.fromPairs(queryParams: _*))
  }

  /** Build the JSON API endpoint for an upload to a GCS bucket. */
  def baseGcsUploadUri(bucket: String, uploadType: String): Uri = {
    val uri = uri"https://www.googleapis.com/upload/storage/v1/b" / bucket / "o"
    uri.withQueryParam("uploadType", uploadType)
  }

  private[gcs] val bytesPerKib = 1024
  private[gcs] val bytesPerMib = 1024 * bytesPerKib

  /**
    * Number of bytes to pull / write from GCS at a time.
    *
    * Google doesn't recommend a particular size for pulling data, but
    * it does say upload chunks should be in multiples of 256KB. We've
    * seen repeated pulls for chunk sizes that are "too small" cause
    * hangs on the Google end too, so fixing a big enough default
    * seems reasonable.
    */
  val ChunkSize: Int = 256 * bytesPerKib

  /**
    * Max number of bytes to send in a single request to GCS.
    *
    * Google recommends this as the threshold for when to switch from a one-shot upload
    * to a resumable upload.
    */
  val MaxBytesPerUploadRequest: Int = 5 * bytesPerMib

  /**
    * Custom GCS header used when initializing a resumable upload to indicate the total
    * number of bytes that will be pushed over the course of that upload.
    */
  val UploadContentLengthHeader = "X-Upload-Content-Length"

  /**
    * Custom GCS header used when initializing a resumable upload to set the expected
    * content-type of the bytes pushed over the course of that upload.
    */
  val UploadContentTypeHeader = "X-Upload-Content-Type"

  /**
    * Custom GCS header sent in the response of successfully-initialized resumable uploads.
    *
    * The value of this header serves as the unique ID for the upload within its enclosing
    * bucket, and must be included in subsequent upload requests.
    */
  val UploadIDHeader = "X-GUploader-UploadID"

  /**
    * Key used in GCS object metadata to report the md5 of the object's contents.
    *
    * @see https://cloud.google.com/storage/docs/hashes-etags
    */
  val ObjectMd5Key: String = "md5Hash"

  /** Key used in GCS object metadata to report the total size of the object. */
  val ObjectSizeKey: String = "size"

  /** Key used in GCS object metadata to report the encoding of an object's contents. */
  val ObjectEncodingKey: String = "contentEncoding"

  /**
    * Status code returned by GCS when a request to upload bytes to a resumable
    * upload succeeds, but the server still expects more bytes.
    */
  val ResumeIncompleteStatus = Status(308, "Resume Incomplete")

  /**
    * Build object metadata to include in upload requests to GCS.
    *
    * @param path the path of the object that will be uploaded, relative to its
    *             enclosing bucket
    * @param expectedMd5 an optional md5 which the object's contents should match
    *                    after the upload completes. Triggers server-side content
    *                    validation if included in the upload request
    */
  private[gcs] def buildUploadMetadata(
    path: String,
    expectedMd5: Option[String]
  ): Json = {
    // Object metadata is used by Google to register the upload to the correct pseudo-path.
    val baseObjectMetadata = JsonObject("name" -> path.asJson)
    expectedMd5
      .fold(baseObjectMetadata) { hexMd5 =>
        baseObjectMetadata.add(
          ObjectMd5Key,
          Base64.encodeBase64String(Hex.decodeHex(hexMd5.toCharArray)).asJson
        )
      }
      .asJson
  }
}
