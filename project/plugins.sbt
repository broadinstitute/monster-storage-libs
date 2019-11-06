// HACKERY: We have to hard-code the organization path in the pattern, and force-overwrite the
// expected patterns, to handle all of the work-arounds we needed to use to get the plugins
// publshing to Artifactory in the first place.
val patternBase =
  "org/broadinstitute/monster/[module](_[scalaVersion])(_[sbtVersion])/[revision]"

val publishPatterns = Patterns()
  .withIsMavenCompatible(false)
  .withIvyPatterns(Vector(s"$patternBase/ivy-[revision].xml"))
  .withArtifactPatterns(Vector(s"$patternBase/[module]-[revision](-[classifier]).[ext]"))

resolvers += Resolver.url(
  "Broad Artifactory",
  new URL("https://broadinstitute.jfrog.io/broadinstitute/libs-release/")
)(publishPatterns)

addSbtPlugin("org.broadinstitute.monster" % "monster-sbt-plugins" % "0.4.0")
