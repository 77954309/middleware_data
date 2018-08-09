
name := "middleware_data"

version := "0.1"

scalaVersion := "2.11.11"

libraryDependencies ++= {
  val sparkVersion = "2.0.2"
  val hadoopVersion = "2.7.4"
  val hiveVersion = "1.2.2"
  Seq(
    "org.apache.spark"    %% "spark-core"             % sparkVersion,
    "org.apache.spark"    %% "spark-sql"               % sparkVersion,
    "org.apache.spark"    %% "spark-streaming"        % sparkVersion,
    "org.apache.spark"    %% "spark-hive"        % sparkVersion,
    "org.apache.hadoop"     %  "hadoop-client"          % hadoopVersion,
    "org.apache.hadoop"     %  "hadoop-common"          % hadoopVersion,
    "org.apache.hive"       %  "hive-hbase-handler"          % hiveVersion,
    "org.apache.hive"       %  "hive-exec"          % hiveVersion  excludeAll ExclusionRule(name = "pentaho-aggdesigner-algorithm"),
    "org.apache.hive"       %  "hive-jdbc"          % hiveVersion,
    "org.apache.hive"       %  "hive-service"          % hiveVersion,
    "org.scalikejdbc"       %  "scalikejdbc_2.11"          % "2.5.0",
    "org.scalikejdbc"       %  "scalikejdbc-config_2.11"          % "2.5.0"
  )
}


libraryDependencies += "org.apache.hbase" % "hbase-client" % "1.3.1"

libraryDependencies += "org.apache.hbase" % "hbase-common" % "1.3.1"

libraryDependencies += "org.apache.hbase" % "hbase-server" % "1.3.1"

libraryDependencies += "com.google.guava"   %  "guava"    % "16.0"

libraryDependencies += "com.alibaba"   %  "fastjson"    % "1.2.8"

libraryDependencies += "mysql"   %  "mysql-connector-java"    % "5.1.38"

libraryDependencies += "org.apache.zookeeper"  %  "zookeeper"   % "3.4.10"

libraryDependencies += "org.slf4j"              %  "slf4j-api"          % "1.7.18"

libraryDependencies += "org.apache.logging.log4j"       %  "log4j-slf4j-impl"          % "2.8.1"

libraryDependencies += "org.apache.logging.log4j"       %  "log4j-core"          % "2.8.1"

libraryDependencies += "com.google.code.gson"       %  "gson"          % "2.8.2"


assemblyMergeStrategy in assembly := {

  case PathList("javax", "servlet", xs@_*) => MergeStrategy.last

  case PathList("javax", "activation", xs@_*) => MergeStrategy.last

  case PathList("org", "apache", xs@_*) => MergeStrategy.last

  case PathList("org", "w3c", xs@_*) => MergeStrategy.last

  case PathList("com", "google", xs@_*) => MergeStrategy.last

  case PathList("com", "codahale", xs@_*) => MergeStrategy.last

  case PathList(ps@_*) if ps.last endsWith ".html" => MergeStrategy.first

  case "log4j.properties" => MergeStrategy.last

  case x =>

    val oldStrategy = (assemblyMergeStrategy in assembly).value

    oldStrategy(x)

}