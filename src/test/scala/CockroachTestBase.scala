package com.lambdalab.jgit.jdbc.test

import com.lambdalab.jgit.jdbc.{JdbcDfsRepository, PostgresRepoBuilder}
import scalikejdbc.ConnectionPool

/**
  * Created by IntelliJ IDEA.
  * User: draco
  * Date: 2018/1/14
  * Time: 下午1:58
  */
trait CockroachTestBase {
  var dfsRepo: JdbcDfsRepository =_

  protected def initRepo(): JdbcDfsRepository = {
    val builder =new PostgresRepoBuilder()
    builder.setDBName('cockroach)
    builder.setRepoName("test")
    dfsRepo = builder.build()
    return dfsRepo
  }

  val dbname = "test"
  val port = 26257
  val url = s"jdbc:postgresql://localhost:$port/$dbname"
  val user = "root"
  val password = ""

  var container: String = null

  def start() : Unit = {
    container =  DockerTool.startContainer("jgit-cockroach", "cockroachdb/cockroach:v1.1.4",
      Map(26257 -> port, 28080 -> 28080),
      Map(
      ),
      "start", "--insecure"
    )
    DockerTool.tailContainer(container, "CockroachDB node starting")
    Class.forName("org.postgresql.Driver")
    ConnectionPool.add('cockroach, url, user, password)
  }

  def stop() : Unit = {
    // DockerTool.stopContainer(container)
  }
}
