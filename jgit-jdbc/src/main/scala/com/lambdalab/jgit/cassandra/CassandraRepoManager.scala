package com.lambdalab.jgit.cassandra

import java.util

import com.lambdalab.jgit.JGitRepoManager

class CassandraRepoManager(val settings: CassandraSettings) extends JGitRepoManager[CassandraDfsRepo] with CassandraContext {

  def init(): Unit = {
    CassandraRefs.createSchema(settings)
    CassandraPacks.createSchema(settings)
  }

  override def isRepoExists(name: String): Boolean = {
    val cql = "select repo from refs where repo=? limit 1;"
    val rs = execute(cql)(_.bind(name)).one()
    rs !=null
  }

  override def createRepo(name: String): CassandraDfsRepo = {
    val repo = openRepo(name)
    repo.create()
    repo
  }

  override def openRepo(name: String): CassandraDfsRepo = {
    val builder = new CassandraRepoBuilder()
        .setCluster(settings.cluster)
        .setKeyspace(settings.keyspace)
        .setRepoName(name)
    builder.build()
  }

  override def deleteRepo(name: String) = {
    openRepo(name).clearRepo(false)
  }

  override def allRepoNames(): java.util.Iterator[String] with AutoCloseable= {
    val cql = "select distinct repo from refs"
    val iterator = execute(cql)(_.bind()).iterator()
    val ret = new util.Iterator[String] with AutoCloseable{
      override def next(): String = iterator.next().getString(0)
      override def hasNext: Boolean = iterator.hasNext

      override def close(): Unit = {}
    }
    ret
  }
}
