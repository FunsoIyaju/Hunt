name := "Hunt"

version := "0.1"

scalaVersion := "2.11.0"

libraryDependencies ++= Seq(
  "org.apache.spark"  %%  "spark-core"    % "2.2.0",
  "org.apache.spark"  %%  "spark-sql"     % "2.2.0",
  "org.apache.spark"  %%  "spark-mllib"   % "2.2.0",
  "org.apache.spark"  %%  "spark-streaming" % "2.2.0",
  "org.apache.spark"  %  "spark-streaming-kafka-0-8_2.11" % "2.2.0",
  "org.apache.spark"  %  "spark-sql-kafka-0-10_2.11" % "2.2.0"
)