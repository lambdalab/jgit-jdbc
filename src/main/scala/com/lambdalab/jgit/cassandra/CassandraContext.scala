package com.lambdalab.jgit.cassandra

import com.datastax.driver.core.{BoundStatement, Cluster, PreparedStatement}
import com.lambdalab.jgit.utils.PrepareStatementCache


trait CassandraSettings {
  val cluster: Cluster
  val keyspace: String
}

trait CassandraContext {
  val settings: CassandraSettings
  lazy val session = {
    settings.cluster.connect(settings.keyspace)
  }
  val statmentCache = new PrepareStatementCache[PreparedStatement](20)

  def execute(cql: String)(bindFunc: PreparedStatement => BoundStatement) = {
    val stmt = statmentCache.apply(cql)(session.prepare)
    val bound = bindFunc(stmt)
    session.execute(bound)
  }
}
