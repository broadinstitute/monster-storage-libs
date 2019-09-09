package org.broadinstitute.monster.storage.gcs

import cats.effect.IO
import fs2.Stream
import org.broadinstitute.monster.storage.common.FileType
import org.http4s.headers._

/** Client which can perform I/O operations against Google Cloud Storage. */
trait GcsApi {

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
  ): Stream[IO, Byte]

  /**
    * Check if an object exists in GCS.
    *
    * @param bucket name of the GCS bucket to check within
    * @param path the path within `bucket` to check
    * @return a boolean indicating if an object exists at `path` in `bucket`, and the
    *         md5 of the object if Google calculated one during the upload
    */
  def statObject(bucket: String, path: String): IO[(Boolean, Option[String])]

  /**
    * Create a new object in GCS.
    *
    * The method used to create the new object will differ depending on the total
    * number of bytes expected in the data stream, according to GCS recommendations.
    *
    * @param bucket the GCS bucket to create the new object within
    * @param path location in `bucket` where the new object should be created
    * @param contentType HTTP content-type of the bytes in `data`
    * @param expectedSize total number of bytes expected to be contained in `data`
    * @param expectedMd5 expected MD5 hash of the bytes in `data`. If given, server-side
    *                    validation will be enabled on the upload
    * @param data bytes to write into the new file
    */
  def createObject(
    bucket: String,
    path: String,
    contentType: `Content-Type`,
    expectedSize: Long,
    expectedMd5: Option[String],
    data: Stream[IO, Byte]
  ): IO[Unit]

  // TODO: return the bytes uploaded rather than unit
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
  def createObjectOneShot(
    bucket: String,
    path: String,
    contentType: `Content-Type`,
    expectedMd5: Option[String],
    data: Stream[IO, Byte]
  ): IO[Unit]

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
  ): IO[String]

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
  ): IO[Either[Long, Unit]]

  /**
    * Delete an object in GCS if exists.
    *
    * @param bucket name of the bucket containing the object to delete
    * @param path path within `bucket` pointing to the object to delete
    * @return true if an object was actually deleted, otherwise false
    */
  def deleteObject(bucket: String, path: String): IO[Boolean]

  /**
    * List the content of a GCS bucket under a path prefix.
    *
    * Returns a failed Stream if no objects exist under the prefix.
    *
    * @param bucket name of the bucket to list contents within
    * @param path prefix to match listed contents against
    * @param pageSize max number of results to pull from GCS per HTTP request
    */
  def listContents(
    bucket: String,
    path: String,
    pageSize: Int
  ): Stream[IO, (String, FileType)]

  /**
    * Copy a GCS object to another GCS path.
    *
    * Uses Google's more efficient "rewrite" API which shuffles bytes on their backend
    * instead of transferring them through the client. NOTE: This still might not copy
    * all bytes in a single request.
    *
    * @param sourceBucket GCS bucket containing the file to copy
    * @param sourcePath path within `sourceBucket` pointing to the file-to-copy
    * @param targetBucket GCS bucket to copy the file into
    * @param targetPath path within `targetBucket` where the copy should be written
    */
  def copyObject(
    sourceBucket: String,
    sourcePath: String,
    targetBucket: String,
    targetPath: String
  ): IO[Unit]

  /**
    * Run the first step of a copy from one GCS object to another GCS path.
    *
    * If the object is "small enough" / co-located with the target path, the copy
    * will succeed in the single operation. Otherwise a token will be returned for
    * use in subsequent requests.
    *
    * @param sourceBucket GCS bucket containing the file to copy
    * @param sourcePath path within `sourceBucket` pointing to the file-to-copy
    * @param targetBucket GCS bucket to copy the file into
    * @param targetPath path within `targetBucket` where the copy should be written
    *
    * @return a [[Right]] if the copy completes after this operation, otherwise a
    *         [[Left]] containing the token to use in subsequent requests
    */
  def initializeCopy(
    sourceBucket: String,
    sourcePath: String,
    targetBucket: String,
    targetPath: String
  ): IO[Either[String, Unit]]

  /**
    * Continue an in-flight copy of a GCS object to another GCS path.
    *
    * @param sourceBucket GCS bucket containing the file to copy
    * @param sourcePath path within `sourceBucket` pointing to the file-to-copy
    * @param targetBucket GCS bucket to copy the file into
    * @param targetPath path within `targetBucket` where the copy should be written
    * @param prevToken continuation token returned by a previous call to the copy API
    *                  with the same arguments
    *
    * @return a [[Right]] if the copy completes after this operation, otherwise a
    *         [[Left]] containing the token to use in subsequent requests
    */
  def incrementCopy(
    sourceBucket: String,
    sourcePath: String,
    targetBucket: String,
    targetPath: String,
    prevToken: String
  ): IO[Either[String, Unit]]
}
