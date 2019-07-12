package org.broadinstitute.monster.storage.gcs

import java.nio.file.Path

import cats.data.NonEmptyList
import cats.effect.{Clock, ContextShift, IO, Resource}
import cats.implicits._
import fs2.Stream
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
  ): Stream[IO, Byte] = {
    val objectUri = baseGcsUri(bucket, path).withQueryParam("alt", "media")
    /*
     * NOTE: We don't use http4s' `Range` type for this header because when it
     * renders ranges without an endpoint it omits the trailing '-'. The HTTP
     * spec seems to say that both forms are equivalent, but GCS only recognizes
     * the form with the trailing '-'.
     *
     * TODO: Instead of a one-shot request for all the bytes, we can be more
     * sophisticated and send a sequence of requests with a constant range size
     * that we believe is much less likely to flap from transient problems.
     *
     * gsutil uses an approach where they repeatedly try to request all the bytes,
     * and when requests fail they see how much data has actually been transferred
     * and retry from that point.
     */
    val range =
      Header("Range", s"bytes=$fromByte-${untilByte.fold("")(b => (b - 1).toString)}")

    // Tell GCS that we're OK accepting gzipped data to prevent it from trying to
    // decompress on the server-side, because that breaks use of the 'Range' header.
    val accept =
      `Accept-Encoding`(NonEmptyList.of(ContentCoding.identity, ContentCoding.gzip))

    val request = Request[IO](
      method = Method.GET,
      uri = objectUri,
      headers = Headers.of(range, accept)
    )

    Stream.resource(runHttp(request)).flatMap { response =>
      if (response.status.isSuccess && gunzipIfNeeded) {
        response.headers.get(`Content-Encoding`) match {
          case Some(header)
              if header.contentCoding == ContentCoding.gzip || header.contentCoding == ContentCoding.`x-gzip` =>
            response.body.through(fs2.compress.gunzip(GunzipBufferSize))
          case _ => response.body
        }
      } else if (response.status.isSuccess) {
        response.body
      } else {
        val fullBody =
          response.body.compile.toChunk.map(chunk => new String(chunk.toArray[Byte]))
        Stream.eval(fullBody).flatMap { payload =>
          val err =
            s"""Attempt to get object bytes returned ${response.status}:
               |$payload""".stripMargin
          Stream.raiseError[IO](new Exception(err))
        }
      }
    }
  }

  /**
    * Check if an object exists in GCS.
    *
    * @param bucket name of the GCS bucket to check within
    * @param path the path within `bucket` to check
    * @return a boolean indicating if an object exists at `path` in `bucket`, and an
    *         optional md5 value for the object if it exists and is known to GCS
    */
  def statObject(bucket: String, path: String): IO[(Boolean, Option[String])] = {
    val gcsUri = baseGcsUri(bucket, path)
    val gcsReq = Request[IO](method = Method.GET, uri = gcsUri)

    runHttp(gcsReq).use { response =>
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
          (true, objectMetadata("md5Hash").flatMap(_.asString))
        }
      } else {
        IO.raiseError(
          new RuntimeException(
            s"Request for metadata from $gcsUri returned status ${response.status}"
          )
        )
      }
    }
  }

  /**
    * Build object metadata to include in upload requests to GCS.
    *
    * @param path the path of the object that will be uploaded, relative to its
    *             enclosing bucket
    * @param expectedMd5 an optional md5 which the object's contents should match
    *                    after the upload completes. Triggers server-side content
    *                    validation if included in the upload request
    */
  private def buildUploadMetadata(path: String, expectedMd5: Option[String]): Json = {
    // Object metadata is used by Google to register the upload to the correct pseudo-path.
    val baseObjectMetadata = JsonObject("name" -> path.asJson)
    expectedMd5
      .fold(baseObjectMetadata) { hexMd5 =>
        baseObjectMetadata.add(
          "md5Hash",
          Base64.encodeBase64String(Hex.decodeHex(hexMd5.toCharArray)).asJson
        )
      }
      .asJson
  }

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

    val dataHeader =
      s"""--${multipartBoundary.value}
         |${`Content-Type`(MediaType.application.json, Charset.`UTF-8`).renderString}
         |
         |${objectMetadata.noSpaces}
         |
         |--${multipartBoundary.value}
         |${contentType.renderString}
         |""".stripMargin.getBytes

    val dataFooter =
      s"""
         |--${multipartBoundary.value}--""".stripMargin.getBytes

    val fullBody = Stream
      .emits(dataHeader)
      .append(data)
      .append(Stream.emits(dataFooter))

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
          IO.raiseError(new RuntimeException(s"Failed to upload bytes to $path in $bucket"))
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
        `Content-Type`(MediaType.application.json, Charset.`UTF-8`),
        Header("X-Upload-Content-Length", expectedSize.toString),
        contentType.toRaw.copy(
          name = CaseInsensitiveString("X-Upload-Content-Type")
        )
      )
    )

    runHttp(gcsReq).use { response =>
      response.headers
        .get(CaseInsensitiveString("X-GUploader-UploadID"))
        .map{_.value}
        .liftTo[IO](new RuntimeException(s"no upload token for $path in $bucket"))
    }
  }

  /**
    * Upload a chunk of bytes to an ongoing GCS resumable upload.
    *
    * @see https://cloud.google.com/storage/docs/json_api/v1/how-tos/resumable-upload#upload-resumable
    *
    * @param bucket the GCS bucket to upload the data into
    * @param uploadToken the unique ID of the previously-initialized resumable upload
    * @param rangeStart the (zero-indexed) position of the first byte of `chunkData` within
    *                   the source file being uploaded
    * @param chunkData chunk of bytes to upload
    * @return either the number of bytes stored by GCS so far (signalling that the upload is not complete),
    *         or Unit when all bytes have been received and GCS signals the upload is done
    */
  def uploadChunk(
    bucket: String,
    uploadToken: String,
    rangeStart: Long,
    chunkData: Stream[IO, Byte],
    bytesPerUploadRequest: Int = MaxBytesPerUploadRequest
  ): IO[Either[Long, Unit]] = {
    val chunks = chunkData.chunkN(bytesPerUploadRequest)

    chunks.evalMap { chunk =>
      val chunkSize = chunk.size

      val gcsReq =  Request[IO](
        method = Method.PUT,
        uri = baseGcsUploadUri(bucket, "resumable")
          .withQueryParam("upload_id", uploadToken),
        headers = Headers.of(
          `Content-Length`.unsafeFromLong(chunkSize.toLong),
          `Content-Range`(rangeStart, rangeStart + chunkSize - 1)
        ),
        body = chunkData
      )

      runHttp(gcsReq).use { response =>
        if (response.status.code == 308) {
          val bytesReceived = for {
            range <- response.headers.get(`Content-Range`)
            rangeEnd <- range.range.second
          } yield {
            rangeEnd
          }
          IO.pure(Left(bytesReceived.getOrElse(rangeStart + chunkSize)))
        } else if (response.status.isSuccess) {
          IO.pure(Right(()))
        } else {
          IO.raiseError(
            new RuntimeException(
              s"Failed to upload chunk with upload token: $uploadToken"
            )
          )
        }
      }
    }.compile.lastOrError
  }

  /**
    * Delete an object in GCS.
    *
    * TODO: Make this succeed if the object is already deleted, if possible.
    *
    * @param bucket name of the bucket containing the object to delete
    * @param path path within `bucket` pointing to the object to delete
    */
  def deleteObject(bucket: String, path: String): IO[Unit] = {
    val gcsUri = baseGcsUri(bucket, path)
    val gcsReq = Request[IO](method = Method.DELETE, uri = gcsUri)

    runHttp(gcsReq).use { response =>
      if (response.status.isSuccess) {
        IO.unit
      } else {
        IO.raiseError(new RuntimeException(s"Failed to delete object $path in $bucket"))
      }
    }
  }
}

object GcsApi {

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
  def baseGcsUri(bucket: String, path: String): Uri =
    // NOTE: Calls to the `/` method cause the RHS to be URL-encoded.
    // This is the correct behavior in this case, but it can cause confusion.
    uri"https://www.googleapis.com/storage/v1/b/" / bucket / "o" / path

  /** Build the JSON API endpoint for an upload to a GCS bucket. */
  def baseGcsUploadUri(bucket: String, uploadType: String): Uri =
    Uri
      .unsafeFromString(s"https://www.googleapis.com/upload/storage/v1/b/$bucket/o")
      .withQueryParam("uploadType", uploadType)

  private val bytesPerKib = 1024
  private val bytesPerMib = 1024 * bytesPerKib

  /**
    * Max number of bytes to send in a single request to GCS.
    *
    * Google recommends this as the threshold for when to switch from a one-shot upload
    * to a resumable upload.
    */
  val MaxBytesPerUploadRequest: Int = 5 * bytesPerMib

  /**
    * Buffer size for client-side gunzipping of compressed data pulled from GCS.
    *
    * The guidelines for this size aren't super clear. They say:
    *   1. Larger than 8KB
    *   2. Around the size of the largest chunk in the gzipped stream
    *
    * GCS's resumable upload docs say upload chunks should be in multiples of 256KB,
    * so it seems like a safe bet for a magic number.
    */
  val GunzipBufferSize: Int = 256 * bytesPerKib
}
