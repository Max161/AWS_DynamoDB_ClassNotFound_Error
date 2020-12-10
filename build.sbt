name := """play_test"""
organization := "co.aws.example"

version := "1.0-SNAPSHOT"

lazy val root = (project in file(".")).enablePlugins(PlayJava)

scalaVersion := "2.13.3"

libraryDependencies += guice

libraryDependencies += "software.amazon.awssdk" % "dynamodb" % "2.15.41"
libraryDependencies += "software.amazon.awssdk" % "dynamodb-enhanced" % "2.15.41"
