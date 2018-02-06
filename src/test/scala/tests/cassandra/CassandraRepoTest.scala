package tests.cassandra

import com.lambdalab.jgit.cassandra.{CassandraDfsRepo, CassandraRepoManager}
import com.lambdalab.jgit.jdbc.test.TestRepositoryTest
import org.junit.{AfterClass, Before, BeforeClass, Test}
import org.junit.Assert._
import scala.collection.JavaConverters._

class CassandraRepoTest  extends  TestRepositoryTest[CassandraDfsRepo] with CassandraTestBase {
  @Before
  def setup(): Unit = {
    super.setUp()
    val repoManager = new CassandraRepoManager(dfsRepo.cassandraSettings)
    repoManager.init()
    dfsRepo.clearRepo()
   }

  @Test
  def testManager(): Unit ={
    val repoManager = new CassandraRepoManager(dfsRepo.cassandraSettings)
    repoManager.deleteRepo(repoName)
    assertFalse("repo should not exists after delete", repoManager.isRepoExists(repoName))
    repoManager.createRepo(repoName)
    assertTrue("repo should exists after create", repoManager.isRepoExists(repoName))
    val all =repoManager.allRepoNames().asScala.toSet
    assertTrue(all.contains(repoName))
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
