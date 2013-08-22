import AssemblyKeys._ // put this at the top of the file

assemblySettings

name := "HadoopWordCount"

version := "0.1"

scalaVersion := "2.10.1"

mainClass in assembly := Some("com.antonkulik.Sort")

jarName in assembly := "hadoop-word-count.jar"
