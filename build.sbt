import _root_.sbt.Keys._

lazy val generalSettings =
  Seq(
    version := "1.0",
    resolvers ++= Seq(
      Resolver.jcenterRepo,
      Resolver.typesafeRepo("releases"),
      "Oracle Repository" at "http://download.oracle.com/maven"
    ),
    scalaVersion := "2.11.3",
    scalacOptions := Seq(
      "-encoding", "utf8"
    ),
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "2.3.0",
      "org.apache.spark" %% "spark-sql" % "2.3.0"
    )
  )

lazy val assemblySettings = Seq(
  assemblyOutputPath in assembly := file(s"dir_for_JARs/${name.value}.jar"),
  mainClass in assembly := Some("Main"),
  assemblyMergeStrategy in assembly := {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case x => MergeStrategy.last
  }
)

lazy val `spark` = (project in file(".")).aggregate(`task_1`,`task_2`,`task_3`,`task_4`,`task_5`,`task_6`)

lazy val `task_1` = (project in file("task_1"))
  .settings(
    generalSettings,
    assemblySettings
  )
lazy val `task_2` = (project in file("task_2"))
  .settings(
    generalSettings,
    assemblySettings
  )
lazy val `task_3` = (project in file("task_3"))
  .settings(
    generalSettings,
    assemblySettings
  )
lazy val `task_4` = (project in file("task_4"))
  .settings(
    generalSettings,
    assemblySettings
  )
lazy val `task_5` = (project in file("task_5"))
  .settings(
    generalSettings,
    assemblySettings
  )

lazy val `task_6` = (project in file("task_6"))
  .settings(
    generalSettings,
    assemblySettings
  )