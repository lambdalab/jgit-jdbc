package com.lambdalab.jgit.jdbc.test

import com.lambdalab.jgit.jdbc.{JdbcDfsRepository, MysqlRepoBuilder}
import scalikejdbc.ConnectionPool

trait TiDBRepoTestBase {
  var dfsRepo: JdbcDfsRepository =_

  protected def initRepo(): JdbcDfsRepository = {
    val builder = new MysqlRepoBuilder()
    builder.setDBName('tidb)
    builder.setRepoName("test")
    dfsRepo = builder.build()
    dfsRepo
  }

  val dbname = "test"
  val port = 24000
  val url = s"jdbc:mysql://localhost:$port/$dbname"
  val user = "root"
  val password = ""

  var container: String = null

  def start() : Unit = {
    container =  DockerTool.startContainer("jgit-tidb", "pingcap/tidb:latest",
      ports = Map(4000 -> port,
        10080 -> 10080),
      envs = Map()
    )
    DockerTool.tailContainer(container, "Server is running")
    initJdbc()
  }

  def initJdbc() = {
    Class.forName("com.mysql.jdbc.Driver")
    ConnectionPool.add('tidb, url, user, password)
  }

  def stop() : Unit = {
    // DockerTool.stopContainer(container)
  }
}

