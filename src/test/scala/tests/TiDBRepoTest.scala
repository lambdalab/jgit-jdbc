package tests

import com.lambdalab.jgit.jdbc.JdbcDfsRepository
import com.lambdalab.jgit.jdbc.test.{TiDBRepoTestBase, TestRepositoryTest}
import org.junit.Assert._
import org.junit.{AfterClass, Before, BeforeClass, Test}

class TiDBRepoTest extends  TestRepositoryTest[JdbcDfsRepository] with TiDBRepoTestBase {

  @Before
  def setup(): Unit = {
    super.setUp()
    if(!dfsRepo.exists())
      dfsRepo.create()
    dfsRepo.clearRepo()
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