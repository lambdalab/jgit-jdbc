package com.lambdalab.jgit.cassandra

import com.datastax.driver.core.{Cluster, PreparedStatement}
import com.lambdalab.jgit.utils.PrepareStatementCache

/**
  * Created by IntelliJ IDEA.
  * User: draco
  * Date: 2018/1/17
  * Time: 下午3:52
  */
trait CassandraContext {
  val cluster: Cluster
  val keyspace: String
  lazy val session = cluster.connect(keyspace)
  val statmentCache = new PrepareStatementCache[PreparedStatement](20)
}
