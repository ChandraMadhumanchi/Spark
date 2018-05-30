# Spark

Plugin for sbt to create Eclipse project definitions. Please see below for installation details and the Documentation for information about configuring sbteclipse. Information about contribution policy and license can be found below.

For sbt 0.13 and up
Add sbteclipse to your plugin definition file (or create one if doesn't exist). You can use either:

the global file (for version 0.13 and up) at ~/.sbt/0.13/plugins/plugins.sbt
the project-specific file at PROJECT_DIR/project/plugins.sbt
For the latest version:

addSbtPlugin("com.typesafe.sbteclipse" % "sbteclipse-plugin" % "5.2.4")
In sbt use the command eclipse to create Eclipse project files

> eclipse

or 
> sbt eclipse
In Eclipse use the Import Wizard to import Existing Projects into Workspace