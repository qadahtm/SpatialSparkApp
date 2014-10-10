import AssemblyKeys._

name := "SpatialSparkApp"

version := "0.1"

scalaVersion := "2.10.4"

resolvers ++= Seq(
  "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/releases/",
  "Secured Central Repository" at "https://repo1.maven.org/maven2"
)

libraryDependencies ++= Seq(
 "org.apache.spark" %% "spark-core" % "1.1.0" %  "provided",
  "org.apache.spark" %% "spark-streaming-kafka" % "1.1.0" %  "provided",
  "org.apache.spark" %% "spark-streaming-twitter" % "1.1.0" %  "provided",
  "joda-time"		    % "joda-time" 		% "latest.integration",
  "org.joda" 			% "joda-convert" 	% "latest.integration",
  "log4j" % "log4j" % "1.2.14"
)

assemblySettings 

scalacOptions ++= Seq(
  "-unchecked",
  "-deprecation",
  "-Xlint",
  "-Ywarn-dead-code",
  "-language:_",
  "-target:jvm-1.6",
  "-encoding", "UTF-8"
)