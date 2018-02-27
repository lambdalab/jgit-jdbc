package tests.ignite

import com.lambdalab.jgit.ignite.IgniteRepo
import com.lambdalab.jgit.jdbc.test.TestRepositoryTest
import org.junit.{AfterClass, Before, BeforeClass}

class IgniteRepoTest extends TestRepositoryTest[IgniteRepo] with IgniteTestBase {
  @Before
  def setup(): Unit = {
    super.setUp()
    repo.clear()
  }
}


object IgniteRepoTest extends IgniteTestBase {
  @BeforeClass
  def beforeClass(): Unit = {
    this.start()
  }
  @AfterClass
  def afterClass(): Unit = {
    this.stop()
  }
}
