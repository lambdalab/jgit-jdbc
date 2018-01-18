package tests.cassandra

import com.lambdalab.jgit.cassandra.{CassandraDfsRepo, CassandraRepoBuilder}
import com.lambdalab.jgit.jdbc.test.DockerTool

trait CassandraTestBase {
  var dfsRepo: CassandraDfsRepo = _
  val keyspace = "jgit"

  val cassandraBuilder = {
    new CassandraRepoBuilder()
        .setRepoName("test")
        .setKeyspace(keyspace)
        .configCluster(_.addContactPoint("127.0.0.1"))
  }

  protected def initRepo(): CassandraDfsRepo = {
    dfsRepo = cassandraBuilder.build()
    dfsRepo
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
