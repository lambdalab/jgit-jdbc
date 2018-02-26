package com.lambdalab.jgit.cassandra

import java.io.IOException

import com.datastax.driver.core.Cluster
import com.lambdalab.jgit.jdbc.ClearableRepo
import org.eclipse.jgit.internal.storage.dfs.{DfsObjDatabase, DfsRepository, DfsRepositoryBuilder, DfsRepositoryDescription}
import org.eclipse.jgit.lib.{Constants, RefDatabase, RefUpdate}

class CassandraRepoBuilder extends DfsRepositoryBuilder[CassandraRepoBuilder, CassandraDfsRepo]  {
  private var ks: String = "jgit"
  private var cluster:Cluster = _
  def setKeyspace(keyspace: String): this.type = {
    this.ks = keyspace
    this
  }


  def configCluster(func: Cluster.Builder => Unit) = {
    val clusterBuilder = Cluster.builder()
    func(clusterBuilder)
    setCluster(clusterBuilder.build())
    this
  }

  // for java
  def configCluster(consumer: java.util.function.Consumer[Cluster.Builder]) = {
    val clusterBuilder = Cluster.builder()
    consumer.accept(clusterBuilder)
    setCluster(clusterBuilder.build())
    this
  }

  def setCluster(cluster: Cluster) = {
    this.cluster = cluster
    this
  }

  def setRepoName(name: String): this.type = {
    this.setRepositoryDescription(new DfsRepositoryDescription(name))
    this
  }
  lazy val settings:  CassandraSettings = new CassandraSettings {
    override val cluster: Cluster = CassandraRepoBuilder.this.cluster
    override val keyspace: String = CassandraRepoBuilder.this.ks
  }

  override def build() ={
    new CassandraDfsRepo(this)
  }
  def close(): Unit = {
    settings.session.close()
    settings.cluster.close()
  }
}

class CassandraDfsRepo(builder: CassandraRepoBuilder) extends DfsRepository(builder) with ClearableRepo{

  val cassandraSettings = builder.settings

  private val objDatabase = new CassandraObjDB(this)
  private val refDatabase = new CassandraRefDB(this)

  override def getObjectDatabase: DfsObjDatabase = objDatabase

  override def getRefDatabase: RefDatabase = refDatabase

  def clear(): Unit = {
    objDatabase.clear()
    refDatabase.clear()
  }

  private def initRepo = {
    val master = Constants.R_HEADS + Constants.MASTER
    val result = updateRef(Constants.HEAD, true).link(master)
    result match {
      case RefUpdate.Result.NEW | RefUpdate.Result.NO_CHANGE =>
      case _ => throw new IOException(result.name)
    }
  }
}
