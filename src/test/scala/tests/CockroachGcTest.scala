package tests

import com.lambdalab.jgit.jdbc.JdbcDfsRepository
import com.lambdalab.jgit.jdbc.test.{CockroachTestBase, DfsGarbageCollectorTest, PostgresTestBase}
import org.junit.{AfterClass, Before, BeforeClass}

class CockroachGcTest  extends DfsGarbageCollectorTest[JdbcDfsRepository] with CockroachTestBase{
  @Before
  def setup(): Unit = {
    super.setUp()
    if(!dfsRepo.exists())
      dfsRepo.create()

    dfsRepo.clearRepo()
  }
}


object CockroachGcTest extends CockroachTestBase {
  @BeforeClass
  def beforeClass(): Unit = {
    this.start()
  }
  @AfterClass
  def afterClass(): Unit = {
    this.stop()
  }
}