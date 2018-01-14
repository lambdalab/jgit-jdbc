package tests

import com.lambdalab.jgit.jdbc.JdbcDfsRepository
import com.lambdalab.jgit.jdbc.test.{PostgresTestBase, TestRepositoryTest}
import org.junit.{AfterClass, Before, BeforeClass}

class PostgresRepoTest extends  TestRepositoryTest[JdbcDfsRepository] with PostgresTestBase {

  @Before
  def setup(): Unit = {
    super.setUp()
   /* if(!dfsRepo.exists())
      dfsRepo.create()*/
    dfsRepo.clearRepo()
  }

 /* @Test
  def testCreateRepo(): Unit = {
    assertTrue(dfsRepo.exists())
  }*/

}

object PostgresRepoTest extends PostgresTestBase {
  @BeforeClass
  def beforeClass(): Unit = {
    this.start()
  }
  @AfterClass
  def afterClass(): Unit = {
    this.stop()
  }
}

