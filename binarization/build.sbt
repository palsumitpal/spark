name := "Test Project"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.0.0"

mainClass in (Compile, run) := Some("BinarizeAllColumns")