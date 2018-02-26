package tests

import com.lambdalab.jgit.jdbc.{JdbcDfsRepository, MysqlDfsRepository}
import com.lambdalab.jgit.jdbc.test.{TestRepositoryTest, TiDBRepoTestBase}
import org.junit.Assert._
import org.junit.{AfterClass, Before, BeforeClass, Test}

class TiDBRepoTest extends  TestRepositoryTest[MysqlDfsRepository] with TiDBRepoTestBase {

  @Before
  def setup(): Unit = {
    super.setUp()
    if(!dfsRepo.exists())
      dfsRepo.create()
    dfsRepo.clear()
  }

  @Test
  def testTiDBCreateRepo(): Unit = {
    assertTrue(dfsRepo.exists())
  }

}


object TiDBRepoTest extends TiDBRepoTestBase {
  @BeforeClass
  def beforeClass(): Unit = {
    this.start()
  }
  @AfterClass
  def afterClass(): Unit = {
    this.stop()
  }
}