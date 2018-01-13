package com.lambdalab.jgit.jdbc.test

import com.lambdalab.jgit.jdbc.{JdbcDfsBuilder, JdbcDfsRepository}
import org.eclipse.jgit.internal.storage.dfs.DfsRepository
import org.junit.Assert._
import org.junit.{AfterClass, Before, BeforeClass, Test}
import scalikejdbc.ConnectionPool

class MysqlRepoTest extends  TestRepositoryTest[JdbcDfsRepository] {

  lazy val dfsRepo: DfsRepository = this.repo.asInstanceOf[DfsRepository]

  override protected def initRepo(): JdbcDfsRepository =
    JdbcDfsBuilder.createMysqlBuilder("test", 'mysql).build()

  @Before
  def setup(): Unit = {
    super.setUp()
  }

  @Test
  def testMysqlCreateRepo(): Unit = {
    if(!dfsRepo.exists())
      dfsRepo.create()
    assertTrue(dfsRepo.exists())
  }


}

object MysqlRepoTest {
  val dbname = "test"
  val port = 23306
  val url = s"jdbc:mysql://localhost:$port/$dbname"
  val user = "root"
  val password = "example"

  var container: String = null
  @BeforeClass
  def start() : Unit = {
    container =  DockerTool.startContainer("jgit-mysql", "mysql:5.6",
      Map(3306 -> 23306),
      Map(
        "MYSQL_ROOT_PASSWORD" -> password,
        "MYSQL_DATABASE" -> dbname
      )
    )
    DockerTool.tailContainer(container, "ready for connections")
    Class.forName("com.mysql.jdbc.Driver")
    ConnectionPool.add('mysql, url, user, password)
  }
  @AfterClass
  def stop() : Unit = {
   // DockerTool.stopContainer(container)
  }
}
