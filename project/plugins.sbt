// Comment to get more information during initialization
logLevel := Level.Warn

// The Typesafe repository 
resolvers += "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/"

// Use sbt-assembly sbt plugin to actually package jar
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.9.1")
