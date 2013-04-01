import sbt._
import Keys._

import com.github.siasia.WebPlugin.webSettings

object SodaFountainWAR{
  lazy val settings: Seq[Setting[_]] = BuildSettings.buildSettings ++ webSettings ++ Seq(
    libraryDependencies ++= Seq(
      "javax.servlet" % "servlet-api" % "2.5" % "provided",
      "org.mortbay.jetty" % "jetty" % "6.1.22" % "container"
    )
  )
}