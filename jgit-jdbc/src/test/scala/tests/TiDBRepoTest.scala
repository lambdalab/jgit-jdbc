package tests

import com.lambdalab.jgit.jdbc.{JdbcDfsRepository, MysqlDfsRepository}
import com.lambdalab.jgit.jdbc.test.{TestRepositoryTest, TiDBRepoTestBase}
import org.eclipse.jgit.api.Git
import org.eclipse.jgit.lib.Constants
import org.eclipse.jgit.lib.SubmoduleConfig.FetchRecurseSubmodulesMode
import org.eclipse.jgit.transport.{TagOpt, URIish}
import org.junit.Assert._
import org.junit.{AfterClass, Before, BeforeClass, Test}

class TiDBRepoTest extends  TestRepositoryTest[MysqlDfsRepository] with TiDBRepoTestBase {

  @Before
  def setup(): Unit = {
    super.setUp()
    if(!repo.exists())
      repo.create(true)
    repo.clear()
  }

  @Test
  def testTiDBCreateRepo(): Unit = {
    assertTrue(repo.exists())
  }

  @Test
  def testFetchMd(): Unit = {
    import collection.JavaConverters._
    val git = new Git(repo)
    val remoteAdd = git.remoteAdd
      remoteAdd.setName(Constants.DEFAULT_REMOTE_NAME)
      remoteAdd.setUri(new URIish("https://github.com/lambdalab/test-repo.git"))
      remoteAdd.call()
    repo.close()
    repo = repoManager.openRepo(repoName)
    val git2 = new Git(repo)
    val remoteList = git2.remoteList()
    val remotes = remoteList.call().asScala
    remotes.foreach(c => println(c.getURIs))

    val fetchCmd = git2.fetch.setRemoveDeletedRefs(true)
        .setTagOpt(TagOpt.FETCH_TAGS) // Fetch all tags
        .setRemote(Constants.DEFAULT_REMOTE_NAME)
        .setRecurseSubmodules(FetchRecurseSubmodulesMode.YES)
    fetchCmd.call()
  }


}



object TiDBRepoTest extends TiDBRepoTestBase {
  @BeforeClass
  def beforeClass(): Unit = {
    this.start()
  }
  @AfterClass
  def afterClass(): Unit = {
    this.stop()
  }
}