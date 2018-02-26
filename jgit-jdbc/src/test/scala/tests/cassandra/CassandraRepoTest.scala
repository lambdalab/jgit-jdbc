package tests.cassandra

import com.lambdalab.jgit.JGitRepoManager
import com.lambdalab.jgit.cassandra.{CassandraDfsRepo, CassandraRepoManager}
import com.lambdalab.jgit.jdbc.test.TestRepositoryTest
import org.junit.{AfterClass, Before, BeforeClass}

class CassandraRepoTest extends TestRepositoryTest[CassandraDfsRepo] with CassandraTestBase {
  @Before
  def setup(): Unit = {
    super.setUp()
    dfsRepo.clear()
  }

}

object CassandraRepoTest extends CassandraTestBase {
  @BeforeClass
  def beforeClass(): Unit = {
  this.start()
}

  @AfterClass
  def afterClass(): Unit = {
  this.stop()
}
}
