// Settings to apply across the entire build
inThisBuild(
  Seq(
    organization := "org.broadinstitute.monster",
    scalaVersion := "2.12.8",
    // Auto-format
    scalafmtConfig := (ThisBuild / baseDirectory)(_ / ".scalafmt.conf").value,
    scalafmtOnCompile := true,
    // Recommended guardrails
    scalacOptions ++= Seq(
      "-deprecation",
      "-encoding",
      "UTF-8",
      "-explaintypes",
      "-feature",
      "-target:jvm-1.8",
      "-unchecked",
      "-Xcheckinit",
      "-Xfatal-warnings",
      "-Xfuture",
      "-Xlint",
      "-Xmax-classfile-name",
      "200",
      "-Yno-adapted-args",
      "-Ypartial-unification",
      "-Ywarn-dead-code",
      "-Ywarn-extra-implicit",
      "-Ywarn-inaccessible",
      "-Ywarn-infer-any",
      "-Ywarn-nullary-override",
      "-Ywarn-nullary-unit",
      "-Ywarn-numeric-widen",
      "-Ywarn-unused",
      "-Ywarn-value-discard"
    )
  )
)

// Compiler plugins.
val betterMonadicForVersion = "0.3.1"

// Data types & control flow.
val catsVersion = "1.6.0"
val catsEffectVersion = "1.2.0"
val fs2Version = "1.0.4"

// JSON.
val circeVersion = "0.11.1"
val circeDerivationVersion = "0.11.0-M1"

// Logging.
val logbackVersion = "1.2.3"

// Web.
val googleAuthVersion = "0.16.2"
val http4sVersion = "0.20.6"

// Testing.
val googleCloudJavaVersion = "1.84.0"
val scalaMockVersion = "4.2.0"
val scalaTestVersion = "3.0.8"
val vaultDriverVersion = "4.1.0"

// Settings to apply to all sub-projects.
// Can't be applied at the build level because of scoping rules.
val commonSettings = Seq(
  addCompilerPlugin("com.olegpy" %% "better-monadic-for" % betterMonadicForVersion),
  Compile / console / scalacOptions := (Compile / scalacOptions).value.filterNot(
    Set(
      "-Xfatal-warnings",
      "-Xlint",
      "-Ywarn-unused",
      "-Ywarn-unused-import"
    )
  ),
  Compile / doc / scalacOptions += "-no-link-warnings",
  Test / fork := true
)

lazy val `monster-storage-libs` = project
  .in(file("."))
  .aggregate(`gcs-lib`)
  .settings(publish / skip := true)

lazy val `gcs-lib` = project
  .in(file("gcs"))
  .configs(IntegrationTest)
  .enablePlugins(PublishPlugin)
  .settings(commonSettings)
  .settings(
    Defaults.itSettings,
    // Main code.
    libraryDependencies ++= Seq(
      "ch.qos.logback" % "logback-classic" % logbackVersion,
      "com.google.auth" % "google-auth-library-oauth2-http" % googleAuthVersion,
      "io.circe" %% "circe-core" % circeVersion,
      "io.circe" %% "circe-derivation" % circeDerivationVersion,
      "io.circe" %% "circe-parser" % circeVersion,
      "org.http4s" %% "http4s-blaze-client" % http4sVersion
    ),
    // All tests.
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % scalaTestVersion
    ).map(_ % s"${Test.name},${IntegrationTest.name}"),
    // Integration tests only.
    libraryDependencies ++= Seq(
      "com.bettercloud" % "vault-java-driver" % vaultDriverVersion,
      "com.google.cloud" % "google-cloud-storage" % googleCloudJavaVersion
    ).map(_ % IntegrationTest),
    // Pin important transitive dependencies to avoid chaos.
    dependencyOverrides := Seq(
      "co.fs2" %% "fs2-core" % fs2Version,
      "co.fs2" %% "fs2-io" % fs2Version,
      "org.typelevel" %% "cats-core" % catsVersion,
      "org.typelevel" %% "cats-effect" % catsEffectVersion
    )
  )
