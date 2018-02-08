package com.lambdalab.jgit.jdbc.test

import com.lambdalab.jgit.jdbc.{JdbcDfsRepository, PostgresRepoManager}
import scalikejdbc.{ConnectionPool, NamedDB}


trait PostgresTestBase {
  var dfsRepo: JdbcDfsRepository =_


  protected def repoManager(): PostgresRepoManager = {
    new PostgresRepoManager(NamedDB('postgres))
  }

  val dbname = "test"
  val port = 25432
  val url = s"jdbc:postgresql://localhost:$port/$dbname"
  val user = "root"
  val password = "example"

  var container: String = null

  def start() : Unit = {
    container =  DockerTool.startContainer("jgit-postgres", "postgres:9.6-alpine",
      Map(5432 -> port),
      Map(
        "POSTGRES_PASSWORD" -> password,
        "POSTGRES_USER" -> user,
        "POSTGRES_DB" -> dbname
      )
    )
    DockerTool.tailContainer(container, "database system is ready to accept connections")
    initJdbc()
  }

  def initJdbc() = {
    Class.forName("org.postgresql.Driver")
    ConnectionPool.add('postgres, url, user, password)
  }

  def stop() : Unit = {
    // DockerTool.stopContainer(container)
  }
}
