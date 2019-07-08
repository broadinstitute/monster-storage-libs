package org.broadinstitute.monster.storage.gcs

import cats.effect.IO
import fs2.Stream
import org.http4s._
import org.http4s.headers.Authorization
import org.scalatest.{FlatSpec, Matchers}

class GcsApiSpec extends FlatSpec with Matchers {

  private val bucket = "bucket"
  private val path = "the/path"
  private val getUri =
    uri"https://www.googleapis.com/storage/v1/b/bucket/o/the%2Fpath?alt=media"
  private val bodyText =
    s"""Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor
       |incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud
       |exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute
       |irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla
       |pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia
       |deserunt mollit anim id est laborum.""".stripMargin
  private val bodyStream = Stream.emits(bodyText.getBytes.toSeq).covary[IO]

  private val fakeAuth = Authorization(BasicCredentials("foo", "bar"))

  private val auth: GcsAuthProvider =
    (req: Request[IO]) => IO.pure(req.transformHeaders(_.put(fakeAuth)))

  behavior of "GcsApi"

  it should "read entire objects as a stream" in {
    val api = new GcsApi(auth, req => {
      req.method shouldBe Method.GET
      req.uri shouldBe getUri
      req.headers.toList should contain theSameElementsAs List(
        fakeAuth,
        Header("Accept-Encoding", "identity, gzip"),
        Header("Range", "bytes=0")
      )
      Stream.emit(Response[IO](body = bodyStream))
    })

    api
      .readObject(bucket, path)
      .through(fs2.text.utf8Decode)
      .compile
      .toChunk
      .map(_.toArray[String].mkString(""))
      .unsafeRunSync() shouldBe bodyText
  }

  it should "read objects starting at an offset" in {
    val start = 100L
    val api = new GcsApi(auth, req => {
      req.method shouldBe Method.GET
      req.uri shouldBe getUri
      req.headers.toList should contain theSameElementsAs List(
        fakeAuth,
        Header("Accept-Encoding", "identity, gzip"),
        Header("Range", s"bytes=$start")
      )
      Stream.emit(Response[IO](body = bodyStream))
    })

    api
      .readObject(bucket, path, startByte = start)
      .through(fs2.text.utf8Decode)
      .compile
      .toChunk
      .map(_.toArray[String].mkString(""))
      .unsafeRunSync() shouldBe bodyText
  }

  it should "read objects ending before the final byte" in {
    val end = 1000L
    val api = new GcsApi(auth, req => {
      req.method shouldBe Method.GET
      req.uri shouldBe getUri
      req.headers.toList should contain theSameElementsAs List(
        fakeAuth,
        Header("Accept-Encoding", "identity, gzip"),
        Header("Range", s"bytes=0-$end")
      )
      Stream.emit(Response[IO](body = bodyStream))
    })

    api
      .readObject(bucket, path, endByte = Some(end))
      .through(fs2.text.utf8Decode)
      .compile
      .toChunk
      .map(_.toArray[String].mkString(""))
      .unsafeRunSync() shouldBe bodyText
  }

  it should "read slices in the middle of an object" in {
    val start = 100L
    val end = 1000L
    val api = new GcsApi(auth, req => {
      req.method shouldBe Method.GET
      req.uri shouldBe getUri
      req.headers.toList should contain theSameElementsAs List(
        fakeAuth,
        Header("Accept-Encoding", "identity, gzip"),
        Header("Range", s"bytes=$start-$end")
      )
      Stream.emit(Response[IO](body = bodyStream))
    })

    api
      .readObject(bucket, path, startByte = start, endByte = Some(end))
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

    val api = new GcsApi(
      auth,
      _ =>
        Stream.emit(
          Response[IO](
            status = Status.BadRequest,
            body = Stream.emit(err).through(fs2.text.utf8Encode)
          )
        )
    )

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

    val api = new GcsApi(
      auth,
      _ =>
        Stream.emit(
          Response[IO](
            status = Status.BadGateway,
            body = Stream.emit(err).through(fs2.text.utf8Encode)
          )
        )
    )

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
