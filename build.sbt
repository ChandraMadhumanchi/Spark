name := "Sample Project"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion = "2.1.0"


libraryDependencies ++= Seq(

	"org.apache.spark"   %% "spark-core"                % sparkVersion % "provided",
	"org.apache.spark"   %% "spark-sql"                 % sparkVersion % "provided",
	"org.apache.spark"   %% "spark-streaming"           % sparkVersion % "provided"
   
)

// A special option to exclude Scala itself form our assembly JAR, since Spark already bundles Scala.
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

// Configure JAR used with the assembly plug-in
assemblyJarName in assembly := "sampleproject99.jar"

assemblyMergeStrategy in assembly := {
	case PathList("META-INF", xs @_*) => 
	  (xs map {_.toLowerCase}) match { 
	    case ("manifest.mf" :: Nil) | ("index.list" :: Nil) | ("dependencies" :: Nil) => MergeStrategy.discard
	    case _ => MergeStrategy.discard
	  }
	case _ => MergeStrategy.first
}

fork in run := true