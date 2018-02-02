package com.lambdalab.jgit.cassandra

import java.io.IOException

import com.datastax.driver.core.Cluster
import com.lambdalab.jgit.jdbc.ClearableRepo
import org.eclipse.jgit.internal.storage.dfs.{DfsObjDatabase, DfsRepository, DfsRepositoryBuilder, DfsRepositoryDescription}
import org.eclipse.jgit.lib.{Constants, RefDatabase, RefUpdate}

class CassandraRepoBuilder extends DfsRepositoryBuilder[CassandraRepoBuilder, CassandraDfsRepo]  {
  private var ks: String = "jgit"
  def setKeyspace(keyspace: String): this.type = {
    this.ks = keyspace
    this
  }

  val clusterBuilder = Cluster.builder()

  def configCluster(func: Cluster.Builder => Unit) = {
    func(clusterBuilder)
    this
  }

  def setRepoName(name: String): this.type = {
    this.setRepositoryDescription(new DfsRepositoryDescription(name))
    this
  }
  lazy val settings:  CassandraSettings = new CassandraSettings {
    override val cluster: Cluster = clusterBuilder.build()
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

  def clearRepo(init:Boolean = true): Unit = {
    objDatabase.clear()
    refDatabase.clear()
    scanForRepoChanges()
    if(init)
      initRepo
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
