package tests

import com.lambdalab.jgit.jdbc.JdbcDfsRepository
import com.lambdalab.jgit.jdbc.test.DfsGarbageCollectorTest
import org.eclipse.jgit.internal.storage.dfs.{DfsRepositoryDescription, InMemoryRepository}

class MemGCTest extends DfsGarbageCollectorTest[InMemoryRepository] {
  override protected def initRepo(): InMemoryRepository =
    new InMemoryRepository(new DfsRepositoryDescription("test"))
}
