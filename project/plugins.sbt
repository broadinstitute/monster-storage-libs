// Inject build variables into app code.
addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.9.0")
// Code formatting.
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.0.2")
// Dynamically set version based on git tags / hashes.
addSbtPlugin("com.dwijnand" % "sbt-dynver" % "4.0.0")
// Parallelize dependency resolution / download.
addSbtPlugin("io.get-coursier" % "sbt-coursier" % "1.1.0-M13-2")
// Generate coverage reports from tests.
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.6.0")
