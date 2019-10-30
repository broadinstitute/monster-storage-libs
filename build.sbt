// Data types & control flow.
val catsVersion = "2.0.0"
val catsEffectVersion = "2.0.0"
val catsRetryVersion = "0.3.1"
val enumeratumVersion = "1.5.13"
val fs2Version = "2.0.1"

// JSON.
val circeVersion = "0.12.1"
val circeDerivationVersion = "0.12.0-M6"

// Logging.
val logbackVersion = "1.2.3"
val log4CatsVersion = "1.0.0"

// Web.
val commonsCodecVersion = "1.13"
val commonsNetVersion = "3.6"
val googleAuthVersion = "0.17.1"
val http4sVersion = "0.21.0-M5"
val sshJVersion = "0.27.0"

// Testing.
val googleCloudJavaVersion = "1.90.0"
val scalaMockVersion = "4.4.0"
val scalaTestVersion = "3.0.8"
val vaultDriverVersion = "5.0.0"

lazy val `monster-storage-libs` = project
  .in(file("."))
  .aggregate(`common-lib`, `gcs-lib`, `ftp-lib`, `sftp-lib`)
  .settings(publish / skip := true)

lazy val `common-lib` = project
  .in(file("common"))
  .enablePlugins(LibraryPlugin)
  .settings(
    libraryDependencies ++= Seq(
      "com.beachape" %% "enumeratum" % enumeratumVersion
    )
  )

lazy val `gcs-lib` = project
  .in(file("gcs"))
  .enablePlugins(LibraryPlugin)
  .dependsOn(`common-lib`)
  .settings(
    // Main code.
    libraryDependencies ++= Seq(
      "ch.qos.logback" % "logback-classic" % logbackVersion,
      "com.google.auth" % "google-auth-library-oauth2-http" % googleAuthVersion,
      "commons-codec" % "commons-codec" % commonsCodecVersion,
      "io.circe" %% "circe-core" % circeVersion,
      "io.circe" %% "circe-derivation" % circeDerivationVersion,
      "io.circe" %% "circe-parser" % circeVersion,
      "org.http4s" %% "http4s-client" % http4sVersion
    ),
    // All tests.
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % scalaTestVersion
    ).map(_ % s"${Test.name},${IntegrationTest.name}"),
    // Integration tests only.
    libraryDependencies ++= Seq(
      "com.bettercloud" % "vault-java-driver" % vaultDriverVersion,
      "com.google.cloud" % "google-cloud-storage" % googleCloudJavaVersion,
      "org.http4s" %% "http4s-blaze-client" % http4sVersion
    ).map(_ % IntegrationTest),
    // Pin important transitive dependencies to avoid chaos.
    dependencyOverrides := Seq(
      "co.fs2" %% "fs2-core" % fs2Version,
      "co.fs2" %% "fs2-io" % fs2Version,
      "org.typelevel" %% "cats-core" % catsVersion,
      "org.typelevel" %% "cats-effect" % catsEffectVersion
    )
  )

lazy val `ftp-lib` = project
  .in(file("ftp"))
  .enablePlugins(LibraryPlugin)
  .dependsOn(`common-lib`)
  .settings(
    libraryDependencies ++= Seq(
      "ch.qos.logback" % "logback-classic" % logbackVersion,
      "co.fs2" %% "fs2-core" % fs2Version,
      "co.fs2" %% "fs2-io" % fs2Version,
      "commons-net" % "commons-net" % commonsNetVersion,
      "io.chrisdavenport" %% "log4cats-slf4j" % log4CatsVersion
    ),
    // All tests.
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % scalaTestVersion
    ).map(_ % s"${Test.name},${IntegrationTest.name}"),
    // Unit tests only.
    libraryDependencies ++= Seq(
      "org.scalamock" %% "scalamock" % scalaMockVersion
    ).map(_ % Test),
    dependencyOverrides := Seq(
      "org.typelevel" %% "cats-core" % catsVersion,
      "org.typelevel" %% "cats-effect" % catsEffectVersion
    )
  )

lazy val `sftp-lib` = project
  .in(file("sftp"))
  .enablePlugins(LibraryPlugin)
  .dependsOn(`common-lib`)
  .settings(
    libraryDependencies ++= Seq(
      "ch.qos.logback" % "logback-classic" % logbackVersion,
      "co.fs2" %% "fs2-core" % fs2Version,
      "co.fs2" %% "fs2-io" % fs2Version,
      "com.github.cb372" %% "cats-retry-core" % catsRetryVersion,
      "com.github.cb372" %% "cats-retry-cats-effect" % catsRetryVersion,
      "com.hierynomus" % "sshj" % sshJVersion,
      "io.chrisdavenport" %% "log4cats-slf4j" % log4CatsVersion
    ),
    // All tests.
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % scalaTestVersion
    ).map(_ % s"${Test.name},${IntegrationTest.name}"),
    // Unit tests only.
    libraryDependencies ++= Seq(
      "org.scalamock" %% "scalamock" % scalaMockVersion
    ).map(_ % Test),
    dependencyOverrides := Seq(
      "org.typelevel" %% "cats-core" % catsVersion,
      "org.typelevel" %% "cats-effect" % catsEffectVersion
    )
  )
