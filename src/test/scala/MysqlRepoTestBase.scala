package com.lambdalab.jgit.jdbc.test

import com.lambdalab.jgit.jdbc.{JdbcDfsRepository, MysqlRepoBuilder}
import org.junit.{AfterClass, BeforeClass}
import scalikejdbc.ConnectionPool

trait MysqlRepoTestBase {
  var dfsRepo: JdbcDfsRepository =_

  protected def initRepo(): JdbcDfsRepository = {
    val builder = new MysqlRepoBuilder()
    builder.setDBName('mysql)
    builder.setRepoName("test")
    dfsRepo = builder.build()
    dfsRepo
  }

  val dbname = "test"
  val port = 23306
  val url = s"jdbc:mysql://localhost:$port/$dbname"
  val user = "root"
  val password = "example"

  var container: String = null

   def start() : Unit = {
    container =  DockerTool.startContainer("jgit-mysql", "mysql:5.6",
      Map(3306 -> port),
      Map(
        "MYSQL_ROOT_PASSWORD" -> password,
        "MYSQL_DATABASE" -> dbname
      )
    )
    DockerTool.tailContainer(container, "ready for connections")
    Class.forName("com.mysql.jdbc.Driver")
    ConnectionPool.add('mysql, url, user, password)
  }
  def stop() : Unit = {
    // DockerTool.stopContainer(container)
  }
}


