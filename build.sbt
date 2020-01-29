name := "Health-care-stroke-Analytics-DSE-Analytics-Demo"

version := "0.1"

scalaVersion := "2.11.8"

resolvers += Resolver.mavenLocal // for testing
resolvers += "DataStax Repo" at "https://repo.datastax.com/public-repos/"

val dseVersion = "6.7.6"

libraryDependencies += "com.datastax.dse" % "dse-spark-dependencies" % dseVersion % "provided"