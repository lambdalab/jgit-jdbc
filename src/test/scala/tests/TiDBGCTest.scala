package tests

import com.lambdalab.jgit.jdbc.JdbcDfsRepository
import com.lambdalab.jgit.jdbc.test.{DfsGarbageCollectorTest, TiDBRepoTestBase}
import org.junit.{AfterClass, Before, BeforeClass}

class TiDBGCTest extends DfsGarbageCollectorTest[JdbcDfsRepository] with TiDBRepoTestBase {
  @Before
  def setup(): Unit = {
    super.setUp()
    dfsRepo.clearRepo()
  }
}

object TiDBGCTest extends TiDBRepoTestBase {
  @BeforeClass
  def beforeClass(): Unit = {
    this.start()
  }
  @AfterClass
  def afterClass(): Unit = {
    this.stop()
  }
}