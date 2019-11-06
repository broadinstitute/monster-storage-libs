package org.broadinstitute.monster.storage.gcs

import java.nio.file.Path

import cats.data.NonEmptyList
import cats.effect.{Clock, ContextShift, IO, Resource}
import cats.implicits._
import fs2.{Chunk, Pull, Stream}
import io.circe.{Json, JsonObject}
import io.circe.jawn.JawnParser
import io.circe.syntax._
import org.apache.commons.codec.binary.{Base64, Hex}
import org.broadinstitute.monster.storage.common.{
  FileAttributes,
  FileType,
  OperationStatus
}
import org.http4s._
import org.http4s.client.Client
import org.http4s.headers._
import org.http4s.implicits._
import org.http4s.multipart.Boundary
import org.http4s.util.CaseInsensitiveString

/**
  * Concrete implementation of a client that can interact with GCS over its JSON API.
  *
  * @param readChunkSize number of bytes to pull from GCS at a time. Google doesn't recommend
  *                      a particular size for this value, but we've seen repeated calls for
  *                      chunk sizes that are "too small" cause hangs on the Google end
  * @param writeChunkSize number of bytes to send in a single request to GCS. Google recommends
  *                       this be a multiple of 256KiB.
  * @param runHttp function which can transform HTTP requests into HTTP responses
  *                (bracketed by connection-management code)
  */
private[gcs] class JsonHttpGcsApi(
  readChunkSize: Int,
  writeChunkSize: Int,
  runHttp: Request[IO] => Resource[IO, Response[IO]]
) extends GcsApi {
  import JsonHttpGcsApi._

  private val parser = new JawnParser()

  private val applicationJsonContentType =
    `Content-Type`(MediaType.application.json, Charset.`UTF-8`)

  private def getObjectMetadata(
    bucket: String,
    path: String
  ): Resource[IO, Response[IO]] =
    runHttp(Request[IO](method = Method.GET, uri = objectMetadataUri(bucket, path)))

  override def readObject(
    bucket: String,
    path: String,
    fromByte: Long = 0L,
    untilByte: Option[Long] = None,
    gunzipIfNeeded: Boolean = false
  ): Stream[IO, Byte] =
    readObjectByChunks(bucket, path, fromByte, untilByte, gunzipIfNeeded)

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
    gunzipIfNeeded: Boolean
  ): Stream[IO, Byte] = {
    val objectUri = objectDataUri(bucket, path)

    def pullBytes(startByte: Long, endByte: Long): Stream[IO, Byte] = {
      val finalRequest = endByte - startByte - 1 <= readChunkSize
      val rangeEnd = if (finalRequest) endByte else startByte + readChunkSize
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
            GcsFailure
              .raise(response, s"Failed to get object bytes from $path in $bucket")
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
          objectMetadata <- parser.parseByteBuffer(byteChunk.toByteBuffer).liftTo[IO]
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
        GcsFailure.raise(response, s"Failed to get object metadata from $path in $bucket")
      }
    }

    Stream.eval(getObjectInfo).flatMap {
      case (objectSize, encoding) =>
        val endByte = untilByte.getOrElse(objectSize)
        val rawBytes = pullBytes(fromByte, endByte)

        if (gunzipIfNeeded && encoding.contains(ContentCoding.gzip)) {
          rawBytes.through(fs2.compress.gunzip(readChunkSize))
        } else {
          rawBytes
        }
    }
  }

  override def statObject(bucket: String, path: String): IO[Option[FileAttributes]] =
    getObjectMetadata(bucket, path).use { response =>
      if (response.status == Status.NotFound) {
        IO.pure(None)
      } else if (response.status.isSuccess) {
        response.body.compile.toChunk.flatMap { byteChunk =>
          val parseMetadata = for {
            objectMetadata <- parser.parseByteBuffer(byteChunk.toByteBuffer)
            objectCursor = objectMetadata.hcursor
            objectSize <- objectCursor.get[Long](ObjectSizeKey)
            objectMd5 <- objectCursor.get[Option[String]](ObjectMd5Key)
            hexMd5 <- objectMd5.fold[Either[Throwable, Option[String]]](Right(None)) {
              b64Md5 =>
                Either.catchNonFatal(
                  Some(Hex.encodeHexString(Base64.decodeBase64(b64Md5)))
                )
            }
          } yield {
            Some(FileAttributes(objectSize, hexMd5))
          }
          parseMetadata.liftTo[IO]
        }
      } else {
        GcsFailure.raise(response, s"Failed to get object metadata from $path in $bucket")
      }
    }

  override def createObject(
    bucket: String,
    path: String,
    contentType: `Content-Type`,
    expectedSize: Long,
    expectedMd5: Option[String],
    data: Stream[IO, Byte]
  ): IO[Unit] = {
    if (expectedSize < writeChunkSize) {
      createObjectOneShot(bucket, path, contentType, expectedMd5, data)
    } else {
      for {
        uploadToken <- initResumableUpload(
          bucket,
          path,
          contentType,
          expectedSize,
          expectedMd5
        )
        upload <- uploadBytes(bucket, uploadToken, 0L, data)
        output <- upload.fold(
          bytesUploaded =>
            IO.raiseError(
              new Exception(
                s"Expected to upload $expectedSize bytes to $path in $bucket but instead uploaded $bytesUploaded"
              )
            ),
          _ => IO.unit
        )
      } yield {
        output
      }
    }
  }

  override def createObjectOneShot(
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
        `Content-Type`(
          MediaType.multipartType(MultipartUploadSubtype, Some(multipartBoundary.value))
        ),
        `Content-Length`.unsafeFromLong(chunk.size.toLong)
      )

      val gcsReq = Request[IO](
        method = Method.POST,
        uri = multipartUploadUri(bucket),
        headers = fullHeaders,
        body = Stream.chunk(chunk)
      )
      runHttp(gcsReq).use { response =>
        if (response.status.isSuccess) {
          IO.unit
        } else {
          GcsFailure.raise(response, s"Failed to create object for $path in $bucket")
        }
      }
    }
  }

  override def initResumableUpload(
    bucket: String,
    path: String,
    contentType: `Content-Type`,
    expectedSize: Long,
    expectedMd5: Option[String]
  ): IO[String] = {
    val objectMetadata = buildUploadMetadata(path, expectedMd5).noSpaces.getBytes

    val gcsReq = Request[IO](
      method = Method.POST,
      uri = resumableUploadUri(bucket, id = None),
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
        .map(_.value)
        .liftTo[IO](
          new RuntimeException(
            s"No upload token returned after initializing upload to $path in $bucket"
          )
        )
    }
  }

  override def uploadBytes(
    bucket: String,
    uploadToken: String,
    rangeStart: Long,
    data: Stream[IO, Byte]
  ): IO[Either[Long, Unit]] =
    uploadByteChunks(bucket, uploadToken, rangeStart, writeChunkSize, data)

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
              uri = resumableUploadUri(bucket, Some(uploadToken)),
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
                  GcsFailure.raise(
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

  override def deleteObject(bucket: String, path: String): IO[Boolean] = {
    val gcsUri = objectUri(bucket, path)
    val gcsReq = Request[IO](method = Method.DELETE, uri = gcsUri)

    runHttp(gcsReq).use { response =>
      if (response.status.isSuccess || response.status == Status.NotFound) {
        IO.pure(response.status.isSuccess)
      } else {
        GcsFailure.raise(response, s"Failed to delete object $path in $bucket")
      }
    }
  }

  override def listContents(
    bucket: String,
    path: String,
    pageSize: Int
  ): Stream[IO, (String, FileType)] = {
    // Helper for pulling a single page of list results.
    def pullNextPage(
      maybePrevToken: Option[String]
    ): IO[(Chunk[(String, FileType)], Option[String])] = {
      val request = Request[IO](
        method = Method.GET,
        uri = listUri(bucket, path, pageSize, maybePrevToken)
      )
      runHttp(request).use { response =>
        if (response.status.isSuccess) {
          response.body.compile.toChunk.flatMap { byteChunk =>
            // Parse the pieces we need out of the response JSON.
            val nextOrError = for {
              listResults <- parser.parseByteBuffer(byteChunk.toByteBuffer)
              resultCursor = listResults.hcursor
              maybeDirs <- resultCursor.get[Option[Vector[String]]](ListPrefixesKey)
              dirs = maybeDirs.getOrElse(Vector.empty)
              maybeFiles <- resultCursor.get[Option[Vector[Json]]](ListResultsKey)
              files = maybeFiles.getOrElse(Vector.empty)
              fileNames <- files.traverse(_.hcursor.get[String](ObjectNameKey))
              nextPageToken <- resultCursor.get[Option[String]](ListTokenKey)
            } yield {
              val outputs = List(
                fileNames -> FileType.File,
                // Prefixes aren't really directories in GCS, but this gives downstream
                // consumers something meaningful to use when distinguishing the two.
                dirs -> FileType.Directory
              ).map {
                case (names, fileType) => Chunk.vector(names).map(_ -> fileType)
              }
              Chunk.concat(outputs) -> nextPageToken
            }
            nextOrError.liftTo[IO]
          }
        } else {
          GcsFailure.raise(response, s"Failed to list contents of $bucket at path $path")
        }
      }
    }

    // Helper for converting the results of listing a single page into the types
    // expected by `unfoldChunkEval`
    def wrapPageResults(
      results: (Chunk[(String, FileType)], Option[String])
    ): Option[(Chunk[(String, FileType)], OperationStatus)] = {
      val (outChunk, maybeNextToken) = results
      val nextStatus = maybeNextToken
        .fold[OperationStatus](OperationStatus.Done)(OperationStatus.InProgress)
      Some(outChunk -> nextStatus)
    }

    Stream
      .unfoldChunkEval(OperationStatus.NotStarted: OperationStatus) {
        case OperationStatus.NotStarted =>
          pullNextPage(None).map(wrapPageResults)
        case OperationStatus.InProgress(token) =>
          pullNextPage(Some(token)).map(wrapPageResults)
        case OperationStatus.Done =>
          IO.pure(None)
      }
      // Make sure there's at least one element in the result stream.
      .pull
      .uncons1
      .flatMap {
        case None =>
          Pull.raiseError[IO](
            new RuntimeException(s"No objects found in $bucket under $path")
          )
        case Some((first, rest)) => rest.cons1(first).pull.echo
      }
      .stream
  }

  override def copyObject(
    sourceBucket: String,
    sourcePath: String,
    targetBucket: String,
    targetPath: String
  ): IO[Unit] = {
    // Helper for converting the results of each copy call into the types expected
    // by `unfoldChunkEval`.
    def handleStepResult(
      result: Either[String, Unit]
    ): Option[(Chunk[Nothing], OperationStatus)] = Some(
      Chunk.empty -> result.fold(OperationStatus.InProgress, _ => OperationStatus.Done)
    )

    Stream
      .unfoldChunkEval(OperationStatus.NotStarted: OperationStatus) {
        case OperationStatus.NotStarted =>
          initializeCopy(sourceBucket, sourcePath, targetBucket, targetPath)
            .map(handleStepResult)
        case OperationStatus.InProgress(token) =>
          incrementCopy(sourceBucket, sourcePath, targetBucket, targetPath, token)
            .map(handleStepResult)
        case OperationStatus.Done =>
          IO.pure(None)
      }
      .compile
      .drain
  }

  override def initializeCopy(
    sourceBucket: String,
    sourcePath: String,
    targetBucket: String,
    targetPath: String
  ): IO[Either[String, Unit]] =
    copyStep(sourceBucket, sourcePath, targetBucket, targetPath, None)

  override def incrementCopy(
    sourceBucket: String,
    sourcePath: String,
    targetBucket: String,
    targetPath: String,
    prevToken: String
  ): IO[Either[String, Unit]] =
    copyStep(sourceBucket, sourcePath, targetBucket, targetPath, Some(prevToken))

  /** Send a request to the GCS API to start/continue a GCS-internal copy. */
  private def copyStep(
    sourceBucket: String,
    sourcePath: String,
    targetBucket: String,
    targetPath: String,
    prevToken: Option[String]
  ): IO[Either[String, Unit]] = {
    val request = Request[IO](
      method = Method.POST,
      uri = rewriteUri(sourceBucket, sourcePath, targetBucket, targetPath, prevToken)
    )

    runHttp(request).use { response =>
      if (response.status.isSuccess) {
        response.body.compile.toChunk.flatMap { byteChunk =>
          val next = for {
            copyResponse <- parser.parseByteBuffer(byteChunk.toByteBuffer)
            nextToken <- copyResponse.hcursor.get[Option[String]](CopyTokenKey)
          } yield {
            nextToken.toLeft(())
          }
          next.liftTo[IO]
        }
      } else {
        GcsFailure.raise(
          response,
          s"Failed to copy $sourcePath in $sourceBucket to $targetPath in $targetBucket"
        )
      }
    }
  }
}

object JsonHttpGcsApi {
  private[gcs] val bytesPerMib = 1024 * 1024

  /** Default chunk size used for reading data from GCS. */
  val DefaultReadChunkSize: Int = 128 * bytesPerMib

  /** Default chunk size used for writing data to GCS. */
  val DefaultWriteChunkSize: Int = 5 * bytesPerMib

  /** Multipart sub-type to include in Content-Type headers of GCS multipart upload requests. */
  val MultipartUploadSubtype = "related"

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

  /** Key used in GCS object metadata to report the filesystem-like path of the object within its bucket. */
  val ObjectNameKey: String = "name"

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

  /** Key which stores full object paths in responses to GCS "list objects" requests. */
  val ListResultsKey: String = "items"

  /** Key which stores directory-like object prefixes in responses to GCS "list objects" commands. */
  val ListPrefixesKey: String = "prefixes"

  /** Key which stores the token for the next page of results in responses to GCS "list objects" commands.  */
  val ListTokenKey: String = "nextPageToken"

  /** Key which stores the token for transferring the next set of bytes in responses to GCS "rewrite" commands. */
  val CopyTokenKey: String = "rewriteToken"

  /**
    * Status code returned by GCS when a request to upload bytes to a resumable
    * upload succeeds, but the server still expects more bytes.
    */
  val ResumeIncompleteStatus: Status = Status(308, "Resume Incomplete")

  /** Base URI for GCS APIs which reference a specific bucket or object. */
  private[this] val GcsReferenceUri: Uri =
    uri"https://www.googleapis.com/storage/v1/b"

  /** Base URI for GCS APIs which create a new object inside a bucket. */
  private[this] val GcsUploadUri: Uri =
    uri"https://www.googleapis.com/upload/storage/v1/b"

  /** Build the JSON API endpoint for an existing bucket/path in GCS. */
  private[this] def gcsUri(
    bucket: String,
    path: Option[String],
    queryParams: (String, String)*
  ): Uri = {
    // NOTE: Calls to the `/` method cause the RHS to be URL-encoded.
    // This is the correct behavior in this case, but it can cause confusion.
    val bucketUri = GcsReferenceUri / bucket / "o"
    path.fold(bucketUri)(bucketUri / _).copy(query = Query.fromPairs(queryParams: _*))
  }

  /** Build a URI pointing to an object in a bucket. */
  private[gcs] def objectUri(bucket: String, path: String): Uri =
    gcsUri(bucket, Some(path))

  /** Build a URI pointing to an object in a bucket which, when queried, will return the object's metadata. */
  private[gcs] def objectMetadataUri(bucket: String, path: String): Uri =
    gcsUri(bucket, Some(path), "alt" -> "json")

  /** Build a URI pointing to an object in a bucket which, when queried, will return the object's contents. */
  private[gcs] def objectDataUri(bucket: String, path: String): Uri =
    gcsUri(bucket, Some(path), "alt" -> "media")

  /** Build a URI pointing to a bucket which, when queried, will list the bucket's contents under a prefix. */
  private[gcs] def listUri(
    bucket: String,
    path: String,
    pageSize: Int,
    pageToken: Option[String]
  ): Uri = {
    val base = gcsUri(
      bucket,
      None,
      // Return every object starting with the prefix path.
      "prefix" -> path,
      // Run in 'directory mode', causing GCS to truncate returned paths at
      // the delimiter & deduplicate instead of returning the full recursive
      // path of every object.
      "delimiter" -> "/",
      // Paginate results.
      "maxResults" -> pageSize.toString,
      // Tell GCS not to include ACL information in returned payloads.
      "projection" -> "noAcl"
    )
    pageToken.fold(base)(base.withQueryParam("pageToken", _))
  }

  /** Build a URI pointing to a bucket which, when POSTed to, will create a new file. */
  private[gcs] def multipartUploadUri(bucket: String): Uri =
    (GcsUploadUri / bucket / "o").withQueryParam("uploadType", "multipart")

  /** Build a URI pointing to a bucket which, when POST=/PUT-ed to, will interact with a resumable upload. */
  private[gcs] def resumableUploadUri(bucket: String, id: Option[String]): Uri = {
    val base = (GcsUploadUri / bucket / "o").withQueryParam("uploadType", "resumable")
    id.fold(base)(base.withQueryParam("upload_id", _))
  }

  /** Build a URI which, when POST-ed to, will create / continue a copy operation. */
  private[gcs] def rewriteUri(
    sourceBucket: String,
    sourcePath: String,
    targetBucket: String,
    targetPath: String,
    id: Option[String]
  ): Uri = {
    val base = GcsReferenceUri / sourceBucket / "o" / sourcePath / "rewriteTo" / "b" / targetBucket / "o" / targetPath
    id.fold(base)(base.withQueryParam("rewriteToken", _))
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
  private[gcs] def buildUploadMetadata(
    path: String,
    expectedMd5: Option[String]
  ): Json = {
    // Object metadata is used by Google to register the upload to the correct pseudo-path.
    val baseObjectMetadata = JsonObject(ObjectNameKey -> path.asJson)
    expectedMd5
      .fold(baseObjectMetadata) { hexMd5 =>
        baseObjectMetadata.add(
          ObjectMd5Key,
          Base64.encodeBase64String(Hex.decodeHex(hexMd5.toCharArray)).asJson
        )
      }
      .asJson
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
    serviceAccountJson: Option[Path],
    readChunkSize: Int = DefaultReadChunkSize,
    writeChunkSize: Int = DefaultWriteChunkSize
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
      new JsonHttpGcsApi(
        readChunkSize,
        writeChunkSize,
        req => Resource.liftF(auth.addAuth(req)).flatMap(httpClient.run)
      )
    }
}
