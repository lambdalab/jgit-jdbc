package tests

import com.lambdalab.jgit.jdbc.PostgresDfsRepository
import com.lambdalab.jgit.jdbc.test.{DfsGarbageCollectorTest, PostgresTestBase}
import org.junit.{AfterClass, Before, BeforeClass}


class PostgresGcTest  extends DfsGarbageCollectorTest[PostgresDfsRepository] with PostgresTestBase{
  @Before
  def setup(): Unit = {
    super.setUp()

    dfsRepo.clearRepo()
  }
}

object PostgresGcTest extends PostgresTestBase {
  @BeforeClass
  def beforeClass(): Unit = {
    this.start()
  }
  @AfterClass
  def afterClass(): Unit = {
    this.stop()
  }
}
