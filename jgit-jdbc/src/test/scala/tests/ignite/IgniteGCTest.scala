package tests.ignite

import com.lambdalab.jgit.ignite.IgniteRepo
import com.lambdalab.jgit.jdbc.test.DfsGarbageCollectorTest
import org.junit.{AfterClass, Before, BeforeClass}

class IgniteGCTest extends  DfsGarbageCollectorTest[IgniteRepo] with IgniteTestBase {

  @Before
  def setup(): Unit = {
    super.setUp()
    repo.clear()
  }
}

object IgniteGCTest extends IgniteTestBase {
  @BeforeClass
  def beforeClass(): Unit = {
    this.start()
  }
  @AfterClass
  def afterClass(): Unit = {
    this.stop()
  }
}

