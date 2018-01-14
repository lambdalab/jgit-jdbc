package tests

import com.lambdalab.jgit.jdbc.JdbcDfsRepository
import com.lambdalab.jgit.jdbc.test.{DfsGarbageCollectorTest, MysqlRepoTestBase}
import org.junit.{AfterClass, Before, BeforeClass}

class MysqlGCTest extends DfsGarbageCollectorTest[JdbcDfsRepository] with MysqlRepoTestBase {
  @Before
  def setup(): Unit = {
    super.setUp()
    dfsRepo.clearRepo()
  }
}

object MysqlGCTest extends MysqlRepoTestBase {
  @BeforeClass
  def beforeClass(): Unit = {
    this.start()
  }
  @AfterClass
  def afterClass(): Unit = {
    this.stop()
  }
}