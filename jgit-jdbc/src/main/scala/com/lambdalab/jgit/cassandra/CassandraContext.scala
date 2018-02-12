package com.lambdalab.jgit.cassandra

import com.datastax.driver.core.{BoundStatement, Cluster, PreparedStatement}
import com.lambdalab.jgit.utils.PrepareStatementCache


trait CassandraSettings {
  val cluster: Cluster
  val keyspace: String
  lazy val session = {
    cluster.connect(keyspace)
  }
  val statmentCache = new PrepareStatementCache[PreparedStatement](20)
}

trait CassandraContext {
  val settings: CassandraSettings

  def execute(cql: String)(bindFunc: PreparedStatement => BoundStatement) = {
    val stmt = statmentCache.apply(cql)(session.prepare)
    val bound = bindFunc(stmt)
    session.execute(bound)
  }
  def statmentCache = settings.statmentCache
  def session = settings.session
}
