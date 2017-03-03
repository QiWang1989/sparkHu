name := "sparkApp"

scalaVersion := "2.10.5"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.0",
  "org.apache.spark" %% "spark-sql" % "1.6.0",
  "org.apache.spark" %% "spark-hive" % "1.6.0",
  "io.argonaut" %% "argonaut" % "6.1",
  "org.apache.spark" %% "spark-streaming" % "1.6.0",
  "org.apache.spark" %% "spark-streaming-kafka" % "1.6.0",
  ("org.apache.kafka" %% "kafka" % "0.10.0.1")
    .exclude("javax.jms", "jms")
    .exclude("com.sun.jdmk", "jmxtools")
    .exclude("com.sun.jmx", "jmxri")
    .exclude("org.slf4j", "slf4j-log4j12")
    .exclude("log4j", "log4j")
)

assemblyMergeStrategy in assembly := {
 case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
 case m if m.toLowerCase.matches("meta-inf/.*\\.sf$") => MergeStrategy.discard
 case _ => MergeStrategy.first
}