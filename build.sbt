
scalaVersion := "2.12.1"

libraryDependencies ++= Seq(
  "org.apache.logging.log4j" % "log4j-api" % "2.7",
  "org.apache.logging.log4j" % "log4j-core" % "2.7",
  "org.elasticsearch.client" % "transport" % "5.1.1",
  "commons-io" % "commons-io" % "2.5",
  "com.google.code.gson" % "gson" % "2.5",
  "com.google.guava" % "guava" % "20.0",
  "com.typesafe.akka" %% "akka-http" % "10.0.1",
  "com.typesafe.akka" %% "akka-http-spray-json" % "10.0.1",

  "org.mockito" % "mockito-all" % "1.10.19" % Test,
  "junit" % "junit" % "4.7" % Test
)
