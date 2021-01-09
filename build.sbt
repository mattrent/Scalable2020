name := "Scalable2020"

version := "0.1"

scalaVersion := "2.12.12"

val sparkVersion = "3.0.1"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-graphx" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "org.graphstream" % "gs-core" % "1.1.1"

artifactName := {(sv: ScalaVersion, module: ModuleID, artifact: Artifact) => "Scalable2020" + "." + artifact.extension }


assemblyMergeStrategy in assembly := {
	case PathList("META-INF", xs @ _*) => MergeStrategy.discard
	case x => MergeStrategy.first
}