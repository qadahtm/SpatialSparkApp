import AssemblyKeys._

name := "SpatialSparkApp"

version := "0.1"

scalaVersion := "2.10.4"

packSettings

seq(
        // [Optional] Specify mappings from program name -> Main class (full package path)
        packMain := Map(
        "WebServer" -> "ui.Webserver"
        ,"StreamConroller" -> "utils.StreamController"    
	),
        // Add custom settings here
        // [Optional] JVM options of scripts (program name -> Seq(JVM option, ...))
        //packJvmOpts := Map("WebServer" -> Seq("-Xmx1048m")
	//),
        // [Optional] Extra class paths to look when launching a program
        packExtraClasspath := Map("main" -> Seq("${PROG_HOME}/etc")), 
        // [Optional] (Generate .bat files for Windows. The default value is true)
        packGenerateWindowsBatFile := false,
        // [Optional] jar file name format in pack/lib folder (Since 0.5.0)
        //   "default"   (project name)-(version).jar 
        //   "full"      (organization name).(project name)-(version).jar
        //   "no-version" (organization name).(project name).jar
        //   "original"  (Preserve original jar file names)
        packJarNameConvention := "default",
        // [Optional] List full class paths in the launch scripts (default is false) (since 0.5.1)
        packExpandedClasspath := true
      ) 

resolvers ++= Seq(
			"spray repo" at "http://repo.spray.io",
			"spray nightlies" at "http://nightlies.spray.io",
  "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/releases/",
  "Secured Central Repository" at "https://repo1.maven.org/maven2"
)

libraryDependencies ++= Seq(
  "com.typesafe.akka"  %% "akka-actor"       % "2.3.6",
  "com.typesafe.akka"  %% "akka-slf4j"       % "2.3.6",
  "com.typesafe.akka"  %% "akka-remote"       % "2.3.6",
  "io.spray"           %% "spray-can"        % "1.3.2",
  "io.spray"           %% "spray-routing"    % "1.3.2",
  "io.spray"           %% "spray-httpx"    % "1.3.2",
  "io.spray"           %% "spray-json"       % "1.3.0",
 "org.apache.spark" %% "spark-core" % "1.1.0" %  "provided",
 "org.apache.spark" %% "spark-streaming" % "1.1.0" %  "provided",
  "org.apache.spark" %% "spark-streaming-kafka" % "1.1.0" %  "provided",
  "org.apache.spark" %% "spark-streaming-twitter" % "1.1.0" %  "provided",
  "joda-time"		    % "joda-time" 		% "latest.integration",
  "org.joda" 			% "joda-convert" 	% "latest.integration",
  "log4j" % "log4j" % "1.2.14",
  "org.postgresql" % "postgresql" % "9.3-1102-jdbc41",
  "com.hp.hpl.jena" % "jena" % "2.6.4",
  "com.hp.hpl.jena" % "arq" % "2.8.8"
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