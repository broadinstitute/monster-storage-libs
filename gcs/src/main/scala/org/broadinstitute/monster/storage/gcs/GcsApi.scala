package org.broadinstitute.monster.storage.gcs

import java.nio.file.Path

import cats.data.NonEmptyList
import cats.effect.{Clock, ContextShift, IO, Resource}
import fs2.Stream
import org.http4s._
import org.http4s.client.Client
import org.http4s.headers.{`Accept-Encoding`, `Content-Encoding`}

/**
  * Client which can perform I/O operations against Google Cloud Storage.
  *
  * @param runHttp function which can transform HTTP requests into HTTP responses
  *                (bracketed by connection-management code)
  */
class GcsApi private[gcs] (runHttp: Request[IO] => Resource[IO, Response[IO]]) {
  import GcsApi._

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
