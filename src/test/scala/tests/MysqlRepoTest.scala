package tests

import com.lambdalab.jgit.jdbc.JdbcDfsRepository
import com.lambdalab.jgit.jdbc.test.{MysqlRepoTestBase, TestRepositoryTest}
import org.junit.Assert._
import org.junit.{AfterClass, Before, BeforeClass, Test}

class MysqlRepoTest extends  TestRepositoryTest[JdbcDfsRepository] with MysqlRepoTestBase {


  @Before
  def setup(): Unit = {
    super.setUp()
    if(!dfsRepo.exists())
      dfsRepo.create()
    dfsRepo.clearRepo(false)
  }

  @Test
  def testMysqlCreateRepo(): Unit = {
    if(!dfsRepo.exists())
      dfsRepo.create()
    assertTrue(dfsRepo.exists())
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
