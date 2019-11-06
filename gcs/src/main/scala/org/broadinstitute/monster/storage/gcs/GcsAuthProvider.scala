package org.broadinstitute.monster.storage.gcs

import java.nio.file.{Files, Path}
import java.time.Instant

import cats.effect.concurrent.Semaphore
import cats.effect.{Clock, Concurrent, IO}
import com.google.auth.oauth2.{
  GoogleCredentials,
  OAuth2Credentials,
  ServiceAccountCredentials
}
import org.http4s.{AuthScheme, Credentials, Request}
import org.http4s.headers.Authorization

/**
  * Utility which can add authorization info for Google Cloud to outgoing HTTP requests.
  */
trait GcsAuthProvider {
  /** Add authorization for GCP to an HTTP request. */
  def addAuth(request: Request[IO]): IO[Request[IO]]
}

object GcsAuthProvider {

  /**
    * Construct a GCP auth provider wrapping provided service-account JSON, or application
    * default credentials if no JSON is given.
    */
  def build(serviceAccountJson: Option[Path])(
    implicit c: Concurrent[IO],
    clk: Clock[IO]
  ): IO[GcsAuthProvider] =
    for {
      baseCreds <- IO.delay {
        serviceAccountJson.fold(GoogleCredentials.getApplicationDefault) { jsonPath =>
          ServiceAccountCredentials.fromStream(Files.newInputStream(jsonPath))
        }
      }
      scopedCreds <- IO.delay {
        /*
         * Scopes are GCP's legacy, pre-IAM method of controlling service account access
         * to resources. Setting the scope to "cloud-platform" effectively makes the scope
         * check a no-op, allowing us to toggle permissions purely through IAM.
         */
        baseCreds.createScoped("https://www.googleapis.com/auth/cloud-platform")
      }
      provider <- GcsAuthProvider(scopedCreds)
    } yield {
      provider
    }

  /**
    * Construct a GCP auth provider wrapping underlying Google credentials.
    *
    * Exposed for testing so we can inject fake credentials while still covering
    * creation of the semaphore which guards against concurrent refresh.
    */
  private[gcs] def apply(credentials: OAuth2Credentials)(
    implicit c: Concurrent[IO],
    clk: Clock[IO]
  ): IO[GcsAuthProvider] = Semaphore[IO](1L).map(new Impl(credentials, _))

  /**
    * Concrete implementation of a utility that provide authorization headers for requests go GCS.
    *
    * @param googleCreds google creds client, pre-loaded to point at either default credentials
    *                    or service account credentials
    * @param credsLock lock for refreshing the raw google creds
    */
  private class Impl(
    googleCreds: OAuth2Credentials,
    credsLock: Semaphore[IO]
  )(implicit clk: Clock[IO])
      extends GcsAuthProvider {

    override def addAuth(request: Request[IO]): IO[Request[IO]] =
      accessToken.map { bearerToken =>
        val creds = Credentials.Token(AuthScheme.Bearer, bearerToken)
        request.transformHeaders(_.put(Authorization(creds)))
      }

    /**
      * Get an access token from the base Google credentials.
      *
      * The token will be refreshed if expired.
      */
    private def accessToken: IO[String] =
      for {
        now <- clk
          .realTime(scala.concurrent.duration.MILLISECONDS)
          .map(Instant.ofEpochMilli)
        // Lock around the refresh check to prevent double-refreshes.
        _ <- credsLock.acquire.bracket(_ => maybeRefreshToken(now))(
          _ => credsLock.release
        )
        accessToken <- IO.delay(googleCreds.getAccessToken)
      } yield {
        accessToken.getTokenValue
      }

    /**
      * Check if the access token for the base Google credentials has expired,
      * and refresh it if so.
      */
    private def maybeRefreshToken(now: Instant): IO[Unit] = {
      val maybeExpirationInstant = for {
        token <- Option(googleCreds.getAccessToken)
        expiration <- Option(token.getExpirationTime)
      } yield {
        expiration.toInstant
      }

      /*
       * Token is valid if:
       *   1. It's not null, and
       *   2. Its expiration time hasn't passed
       *
       * We pretend the token expires a second early to give some wiggle-room.
       */
      val tokenIsValid = maybeExpirationInstant.exists(_.minusSeconds(1).isAfter(now))

      if (tokenIsValid) {
        IO.unit
      } else {
        IO.delay(googleCreds.refresh())
      }
    }
  }
}
