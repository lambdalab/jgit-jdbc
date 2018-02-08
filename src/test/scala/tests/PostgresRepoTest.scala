package tests

import com.lambdalab.jgit.jdbc.{JdbcDfsRepository, PostgresDfsRepository}
import com.lambdalab.jgit.jdbc.test.{PostgresTestBase, TestRepositoryTest}
import org.junit.{AfterClass, Before, BeforeClass}

class PostgresRepoTest extends  TestRepositoryTest[PostgresDfsRepository] with PostgresTestBase {

  @Before
  def setup(): Unit = {
    super.setUp()
    if(!dfsRepo.exists())
      dfsRepo.create()
    dfsRepo.clearRepo(false)
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

