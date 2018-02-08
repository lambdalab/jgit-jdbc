package tests

import com.lambdalab.jgit.jdbc.MysqlDfsRepository
import com.lambdalab.jgit.jdbc.test.{DfsGarbageCollectorTest, MysqlRepoTestBase}
import org.junit.{AfterClass, Before, BeforeClass}

class MysqlGCTest extends DfsGarbageCollectorTest[MysqlDfsRepository] with MysqlRepoTestBase {
  @Before
  def setup(): Unit = {
    super.setUp()
    if(!repo.exists())
      repo.create()
    repo.clearRepo()
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