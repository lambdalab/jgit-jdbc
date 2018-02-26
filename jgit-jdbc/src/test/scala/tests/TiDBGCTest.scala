package tests

import com.lambdalab.jgit.jdbc.MysqlDfsRepository
import com.lambdalab.jgit.jdbc.test.{DfsGarbageCollectorTest, TiDBRepoTestBase}
import org.junit.{AfterClass, Before, BeforeClass}

class TiDBGCTest extends DfsGarbageCollectorTest[MysqlDfsRepository] with TiDBRepoTestBase {
  @Before
  def setup(): Unit = {
    super.setUp()
    if(!repo.exists())
      repo.create()
    repo.clear()
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