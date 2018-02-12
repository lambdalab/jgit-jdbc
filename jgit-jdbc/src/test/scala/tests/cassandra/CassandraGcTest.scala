package tests.cassandra

import com.lambdalab.jgit.cassandra.CassandraDfsRepo
import com.lambdalab.jgit.jdbc.test.DfsGarbageCollectorTest
import org.junit.{AfterClass, Before, BeforeClass}

class CassandraGcTest  extends  DfsGarbageCollectorTest[CassandraDfsRepo] with CassandraTestBase {
  @Before
  def setup(): Unit = {
    super.setUp()
    dfsRepo.clearRepo()
  }
}

object CassandraGcTest extends CassandraTestBase {
  @BeforeClass
  def beforeClass(): Unit = {
    this.start()
  }
  @AfterClass
  def afterClass(): Unit = {
    this.stop()
  }
}

