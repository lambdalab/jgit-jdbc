package tests.cassandra

import com.datastax.driver.core.Cluster
import com.lambdalab.jgit.JGitRepoManager
import com.lambdalab.jgit.cassandra.{CassandraDfsRepo, CassandraRepoBuilder, CassandraRepoManager, CassandraSettings}
import com.lambdalab.jgit.jdbc.test.DockerTool

trait CassandraTestBase {
  var dfsRepo: CassandraDfsRepo = _


  val cassandraSettings = new CassandraSettings {
    override val cluster: Cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
    override val keyspace: String = "jgit"
  }

  protected def repoManager(): JGitRepoManager[CassandraDfsRepo] = {
    val manager = new CassandraRepoManager(cassandraSettings)
    manager.init()
    manager
  }

  val port = 9042

  var container: String = null

  def start(): Unit = {
    container = DockerTool.startContainer("cassandra", "cassandra:latest",
      Map(9042 -> port),
      Map()
    )
    DockerTool.tailContainer(container, "Starting listening for CQL")
  }

  def stop(): Unit = {
    // DockerTool.stopContainer(container)
  }
}
