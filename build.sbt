import AssemblyKeys._

name := "SpatialSparkApp"

version := "0.1"

scalaVersion := "2.10.2"

resolvers ++= Seq(
  "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/releases/",
	"Secured Central Repository" at "https://repo1.maven.org/maven2"
)

//externalResolvers := Resolver.withDefaultResolvers(resolvers.value, mavenCentral = false)

libraryDependencies += ("com.sclasen" %% "akka-kafka" % "0.0.6" % "compile")
						.exclude("org.jboss.netty","netty")
						//.exclude("com.typesafe.akka","akka-actor_2.10")

libraryDependencies += ("com.typesafe.akka" %% "akka-slf4j" % "2.3.2")
						//.exclude("com.typesafe.akka","akka-actor_2.10")

//libraryDependencies += "org.slf4j" % "log4j-over-slf4j" % "1.6.6" % "compile"

libraryDependencies += "log4j" % "log4j" % "1.2.14"

libraryDependencies ++= Seq(
 "org.apache.spark" %% "spark-core" % "1.0.2" %  "provided",
  "org.apache.spark" %% "spark-streaming-kafka" % "1.0.2" %  "provided",
  "org.apache.spark" %% "spark-streaming-twitter" % "1.0.2" %  "provided",
  //"org.apache.spark" %% "spark-catalyst" % "1.0.2" %  "provided",
  //"org.apache.spark" %% "spark-streaming" % "1.0.2" %  "provided",
  //"org.apache.spark" %% "spark-mllib" % "1.0.2" %  "provided",
  //"org.apache.spark" %% "spark-sql" % "1.0.2" %  "provided",
  //"org.apache.spark" %% "spark-graphx" % "1.0.2" %  "provided"
   "joda-time"		    % "joda-time" 		% "latest.integration",
  "org.joda" 			% "joda-convert" 	% "latest.integration"
)

assemblySettings 

Seq(
    mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
    {
        case PathList("slf4j", "api", xs @ _*)         => MergeStrategy.first
        case PathList("javax", "servlet", xs @ _*)         => MergeStrategy.first
        case PathList("javax", "transaction", xs @ _*)     => MergeStrategy.first
        case PathList("javax", "mail", xs @ _*)     => MergeStrategy.first
        case PathList("javax", "activation", xs @ _*)     => MergeStrategy.first
        case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
        case "application.conf" => MergeStrategy.concat
        case "unwanted.txt"     => MergeStrategy.discard
        case x => old(x)
        }
    })

scalacOptions ++= Seq(
  "-unchecked",
  "-deprecation",
  "-Xlint",
  "-Ywarn-dead-code",
  "-language:_",
  "-target:jvm-1.7",
  "-encoding", "UTF-8"
)

testOptions += Tests.Argument(TestFrameworks.JUnit, "-v")
