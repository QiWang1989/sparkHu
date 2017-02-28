name := "sparkApp"

scalaVersion := "2.10.5"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.0",
  "org.apache.spark" %% "spark-sql" % "1.6.0",
  "org.apache.spark" %% "spark-hive" % "1.6.0",
  "io.argonaut" %% "argonaut" % "6.1",
  "org.apache.spark" % "spark-streaming" % "1.6.0"
)

//assemblyMergeStrategy in assembly := {
//  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
//  case m if m.toLowerCase.matches("meta-inf/.*\\.sf$") => MergeStrategy.discard
//  case _ => MergeStrategy.first
//}