import sbt._
import sbt.Keys._
import sbtdynver.DynVerPlugin

/**
  * Plugin which adds publish configuration for our Artifactory instance
  * to enabled sub-projects.
  */
object PublishPlugin extends AutoPlugin {
  import DynVerPlugin.autoImport._

  // Make sure sbt-dynver is loaded before these settings.
  override def requires: Plugins = DynVerPlugin

  /** Realm reported by our Artifactory instance. */
  private val artifactoryRealm = "Artifactory Realm"

  /** Hostname of our Artifactory instance. */
  private val artifactoryHost = "broadinstitute.jfrog.io"

  /** Environment variable expected to contain a username for our Artifactory. */
  private val artifactoryUsernameVar = "ARTIFACTORY_USERNAME"

  /** Environment variable expected to contain a password for our Artifactory. */
  private val artifactoryPasswordVar = "ARTIFACTORY_PASSWORD"

  /**
    * Credentials which can authenticate the build tool with our Artifactory.
    *
    * We frame this as a setting so that it is loaded once at the start of the build,
    * but within the proper DAG of settings set up by sbt.
    */
  private lazy val artifactoryCredentials = Def.setting {
    val cred = for {
      username <- sys.env.get(artifactoryUsernameVar)
      password <- sys.env.get(artifactoryPasswordVar)
    } yield {
      Credentials(artifactoryRealm, artifactoryHost, username, password)
    }

    cred.orElse {
      // SBT's logging comes from a task, and tasks can't be used inside settings, so we have to roll our own warning...
      println(
        s"[${scala.Console.YELLOW}warn${scala.Console.RESET}] $artifactoryUsernameVar or $artifactoryPasswordVar not set, publishing will fail!"
      )
      None
    }
  }

  /** Maven-style resolver for our Artifactory instance. */
  private lazy val artifactoryResolver = Def.task {
    val target = if (isSnapshot.value) "snapshot" else "release"
    artifactoryRealm at s"https://$artifactoryHost/broadinstitute/libs-$target-local"
  }

  // Settings to add to the entire build when this plugin is loaded on a project.
  override def buildSettings: Seq[Def.Setting[_]] = Seq(
    dynverSonatypeSnapshots := true
  )

  // Settings to add to the specific project when this plugin is loaded on that project.
  override def projectSettings: Seq[Def.Setting[_]] = Seq(
    publishTo := Some(artifactoryResolver.value),
    credentials ++= artifactoryCredentials.value.toSeq
  )
}
