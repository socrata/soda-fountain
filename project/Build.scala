import sbt._
import Keys._

object Build extends sbt.Build {
  lazy val build = Project(
    "soda-fountain",
    file("."),
    settings = BuildSettings.buildSettings
  ) aggregate( sodaFountainLib, sodaFountainWAR, sodaFountainJetty )

  lazy val sodaFountainLib = Project(
    "soda-fountain-lib",
    file("soda-fountain-lib"),
    settings = SodaFountainLib.settings
  )

  lazy val sodaFountainWAR = Project(
    "soda-fountain-war",
    file("soda-fountain-war"),
    settings = SodaFountainWAR.settings
  ) dependsOn(sodaFountainLib)

  lazy val sodaFountainJetty = Project(
    "soda-fountain-jetty",
    file("soda-fountain-jetty"),
    settings = SodaFountainJetty.settings
  ) dependsOn(sodaFountainLib)
}