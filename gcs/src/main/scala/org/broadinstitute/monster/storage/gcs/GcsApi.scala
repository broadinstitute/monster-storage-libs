package org.broadinstitute.monster.storage.gcs

import java.nio.file.Path

import cats.data.NonEmptyList
import cats.effect.{Clock, ContextShift, IO}
import fs2.Stream
import org.http4s._
import org.http4s.client.Client
import org.http4s.headers.{Range, `Accept-Encoding`}

/**
  * Client which can perform I/O operations against Google Cloud Storage.
  *
  * @param authProvider utility which can add GCS-specific authorization headers
  *                     to outgoing HTTP requests
  * @param runHttp function which can transform HTTP requests into HTTP responses
  *                (bracketed by connection-management code)
  */
class GcsApi private[gcs] (
  authProvider: GcsAuthProvider,
  runHttp: Request[IO] => Stream[IO, Response[IO]]
) {
  import GcsApi._

  /**
    * Read a range of bytes (potentially the whole file) from an object in cloud storage.
    *
    * @param bucket name of the GCS bucket to read from
    * @param path path within `bucket` containing the object-to-read
    * @param startByte first byte (zero-indexed) within the object at `path` which should be
    *                  included in the response from GCS
    * @param endByte final byte (zero-indexed) within the object at `path` which should be
    *                included in the response from GCS, or `None` if GCS should attempt to
    *                return all bytes following the start of the range
    */
  def readObject(
    bucket: String,
    path: String,
    startByte: Long = 0L,
    endByte: Option[Long] = None
  ): Stream[IO, Byte] = {
    val objectUri = baseGcsUri(bucket, path).withQueryParam("alt", "media")
    /*
     * TODO: Instead of a one-shot request for all the bytes, we can be more
     * sophisticated and send a sequence of requests with a constant range size
     * that we believe is much less likely to flap from transient problems.
     *
     * gsutil uses an approach where they repeatedly try to request all the bytes,
     * and when requests fail they see how much data has actually been transferred
     * and retry from that point.
     */
    val range = Range(RangeUnit.Bytes, Range.SubRange(startByte, endByte))

    // Tell GCS that we're OK accepting gzipped data to prevent it from trying to
    // decompress on the server-side, because that breaks use of the 'Range' header.
    val accept =
      `Accept-Encoding`(NonEmptyList.of(ContentCoding.identity, ContentCoding.gzip))

    val request = Request[IO](
      method = Method.GET,
      uri = objectUri,
      headers = Headers.of(range, accept)
    )

    Stream
      .eval(authProvider.addAuth(request))
      .flatMap(runHttp)
      .flatMap { response =>
        if (response.status.isSuccess) {
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
   * Construct a GCS client which will delegate to a given HTTP client when sending requests,
   * optionally using service account credentials for authorization instead of the environment's
   * application default credentials.
   *
   * @param httpClient client which will actually send / receive HTTP requests to / from GCS
   * @param serviceAccountJson optional path to service account credentials on disk, to use
   *                           in place of application default credentials
   */
  def build(
    httpClient: Client[IO],
    serviceAccountJson: Option[Path]
  )(implicit cs: ContextShift[IO], clk: Clock[IO]): IO[GcsApi] =
    GcsAuthProvider.build(serviceAccountJson).map(new GcsApi(_, httpClient.stream))

  /** Build the JSON API endpoint for an existing bucket/path in GCS. */
  def baseGcsUri(bucket: String, path: String): Uri =
    // NOTE: Calls to the `/` method cause the RHS to be URL-encoded.
    // This is the correct behavior in this case, but it can cause confusion.
    uri"https://www.googleapis.com/storage/v1/b/" / bucket / "o" / path

  private val bytesPerMib = 1048576

  /**
    * Max number of bytes to send in a single request to GCS.
    *
    * Google recommends this as the threshold for when to switch from a one-shot upload
    * to a resumable upload.
    */
  val MaxBytesPerUploadRequest: Int = 5 * bytesPerMib
}
