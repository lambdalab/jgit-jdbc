package tests

import com.lambdalab.jgit.jdbc.MysqlDfsRepository
import com.lambdalab.jgit.jdbc.test.{MysqlRepoTestBase, TestRepositoryTest}
import org.eclipse.jgit.api.Git
import org.eclipse.jgit.lib.{Constants, TextProgressMonitor}
import org.eclipse.jgit.lib.SubmoduleConfig.FetchRecurseSubmodulesMode
import org.eclipse.jgit.transport.{TagOpt, URIish}
import org.junit.Assert._
import org.junit.{AfterClass, Before, BeforeClass, Test}

class MysqlRepoTest extends TestRepositoryTest[MysqlDfsRepository] with MysqlRepoTestBase {

  @Before
  def setup(): Unit = {
    super.setUp()
    if (!repo.exists())
      repo.create()
    repo.clearRepo(false)
  }

  @Test
  def testMysqlCreateRepo(): Unit = {
    if (!repo.exists())
      repo.create()
    assertTrue(repo.exists())
  }

  @Test
  def testFetchMd(): Unit = {
    //    val r = new InMemoryRepository(new DfsRepositoryDescription("test"))
    val git = new Git(repo)
    val remoteAdd = git.remoteAdd
    remoteAdd.setName(Constants.DEFAULT_REMOTE_NAME)
    remoteAdd.setUri(new URIish("/tmp/test-repo"))
    remoteAdd.call()

    val fetchCmd = git.fetch.setRemoveDeletedRefs(true)
        .setTagOpt(TagOpt.FETCH_TAGS) // Fetch all tags
        .setProgressMonitor(new TextProgressMonitor())
        .setRemote(Constants.DEFAULT_REMOTE_NAME)
        .setRecurseSubmodules(FetchRecurseSubmodulesMode.YES)
    fetchCmd.call()
    repo.close()
  }

}

object MysqlRepoTest extends MysqlRepoTestBase {
  @BeforeClass
  def beforeClass(): Unit = {
    this.start()
  }

  @AfterClass
  def afterClass(): Unit = {
    this.stop()
  }
}
