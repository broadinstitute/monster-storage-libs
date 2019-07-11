package org.broadinstitute.monster.storage.gcs

import java.util.Date
import java.util.concurrent.atomic.AtomicReference

import cats.effect.{Clock, ContextShift, IO}
import cats.implicits._
import com.google.auth.oauth2.{AccessToken, OAuth2Credentials}
import org.http4s.{Header, Request}
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.TimeUnit

class GcsAuthProviderSpec extends FlatSpec with Matchers {

  private implicit val cs: ContextShift[IO] =
    IO.contextShift(ExecutionContext.global)

  private implicit val fixedClock: Clock[IO] = new Clock[IO] {
    override def realTime(unit: TimeUnit): IO[Long] = IO.pure(123L)
    override def monotonic(unit: TimeUnit): IO[Long] = IO.pure(123L)
  }

  behavior of "GcsAuthProvider"

  it should "initialize fresh credentials" in {

    val googleCreds = new OAuth2Credentials {
      override def refreshAccessToken(): AccessToken =
        new AccessToken("the-token", new Date(1500L))
    }

    val request = Request[IO]()

    val withAuth = for {
      provider <- GcsAuthProvider(googleCreds)
      withAuth <- provider.addAuth(request)
    } yield {
      withAuth
    }

    withAuth.unsafeRunSync() shouldBe request.withHeaders(
      Header("Authorization", "Bearer the-token")
    )
  }

  it should "refresh expired credentials" in {

    val token1 = new AccessToken("the-token", new Date(122L))
    val token2 = new AccessToken("the-token2", new Date(1500L))

    val googleCreds = new OAuth2Credentials(token1) {
      override def refreshAccessToken(): AccessToken = token2
    }

    val request = Request[IO]()

    val withAuth = for {
      provider <- GcsAuthProvider(googleCreds)
      withAuth <- provider.addAuth(request)
    } yield {
      withAuth
    }

    withAuth.unsafeRunSync() shouldBe request.withHeaders(
      Header("Authorization", "Bearer the-token2")
    )
  }

  it should "not refresh valid credentials" in {

    val token = new AccessToken("the-token", new Date(1500L))

    val googleCreds = new OAuth2Credentials(token) {
      override def refreshAccessToken(): AccessToken = ???
    }

    val request = Request[IO]()

    val withAuth = for {
      provider <- GcsAuthProvider(googleCreds)
      withAuth <- provider.addAuth(request)
    } yield {
      withAuth
    }

    withAuth.unsafeRunSync() shouldBe request.withHeaders(
      Header("Authorization", "Bearer the-token")
    )
  }

  it should "refresh once on concurrent access" in {

    val initToken = new AccessToken("the-token", new Date(122L))
    val ref = new AtomicReference[AccessToken](initToken)

    val googleCreds: OAuth2Credentials = new OAuth2Credentials(initToken) {
      override def refreshAccessToken(): AccessToken =
        ref.updateAndGet { token =>
          new AccessToken(
            token.getTokenValue + "1",
            new Date(token.getExpirationTime.getTime + 3000L)
          )
        }
    }

    val request = Request[IO]()

    val check = for {
      provider <- GcsAuthProvider(googleCreds)
      addAuth = provider.addAuth(request)
      (_, _, req) <- (addAuth, addAuth, addAuth).parTupled
    } yield {
      req shouldBe request.withHeaders(
        Header("Authorization", "Bearer the-token1")
      )
    }

    check.unsafeRunSync()
  }
}
