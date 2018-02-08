package tests

import com.lambdalab.jgit.JGitRepoManager
import com.lambdalab.jgit.jdbc.{JdbcDfsRepository, JdbcRepoManager, MysqlDfsRepository, MysqlRepoManager}
import com.lambdalab.jgit.jdbc.test.{MysqlRepoTestBase, TestRepositoryTest}
import org.junit.Assert._
import org.junit.{AfterClass, Before, BeforeClass, Test}
import scalikejdbc.NamedDB

class MysqlRepoTest extends  TestRepositoryTest[MysqlDfsRepository] with MysqlRepoTestBase {


  @Before
  def setup(): Unit = {
    super.setUp()
    if(!repo.exists())
      repo.create()
    repo.clearRepo(false)
  }

  @Test
  def testMysqlCreateRepo(): Unit = {
    if(!repo.exists())
      repo.create()
    assertTrue(repo.exists())
  }

}

object MysqlRepoTest extends MysqlRepoTestBase {
  @BeforeClass
  def beforeClass(): Unit = {
    this.start()
  }
  @AfterClass
  def afterClass(): Unit = {
    this.stop()
  }
}
