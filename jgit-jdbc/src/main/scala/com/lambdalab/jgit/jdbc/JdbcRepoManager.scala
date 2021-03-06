package com.lambdalab.jgit.jdbc

import java.util

import com.lambdalab.jgit.JGitRepoManager
import com.lambdalab.jgit.jdbc.schema.{JdbcSchemaSupport, MysqlSchemaSupport, PostgresSchemaSupport}
import scalikejdbc.{NamedDB, _}

abstract class JdbcRepoManager[T <: JdbcDfsRepository] extends JGitRepoManager[T] with JdbcSchemaSupport {

  override def init(): Unit = db localTxWithConnection  {
    implicit conn =>
    if (!isTableExists(refsTableName,conn)) {
      createRefTable(conn)
    }
    if (!isTableExists(packTableName,conn)) {
      createPackTable(conn)
    }
  }

  override def isRepoExists(name: String): Boolean = {
    val conn = ConnectionPool(this.db.name).borrow()
    val stmt = conn.prepareStatement(s"select 1 from ${this.refsTableName} where repo = ? limit 1")
    try {
      stmt.setString(1, name)
      stmt.executeQuery().first()
    } finally {
      stmt.close()
      conn.close()
    }
  }

  override def createRepo(name: String): T = {
    val repository = openRepo(name)
    repository.create()
    repository
  }


  override def deleteRepo(name: String): Unit = {
    val repository = openRepo(name)
    repository.clearRepo(false)
  }

  override def allRepoNames(): util.Iterator[String] with AutoCloseable = {
    val conn = ConnectionPool(this.db.name).borrow()

    val stmt = conn.prepareStatement(s"select distinct repo from ${this.refsTableName}")
    val rs = stmt.executeQuery()
    new util.Iterator[String] with AutoCloseable {
      private var _next: String = _
      private var _hasNext: Boolean = false
      private var loaded: Boolean = false

      override def next(): String = {
        if (!loaded) {
          loadNext()
        }
        loaded = false
        _next
      }

      private def loadNext() = {
        _hasNext = rs.next()
        if (_hasNext) {
          _next = rs.getString(1)
        } else {
          _next = null
        }
        loaded = true
      }

      override def hasNext: Boolean = {
        if (!loaded) {
          loadNext()
        }
        _hasNext
      }

      override def close(): Unit = {
        rs.close()
        stmt.close()
        conn.close()
      }
    }
  }
}
object JdbcRepoManager {

  def createMysql(url: String, username:String, password: String)= {
    ConnectionPool.singleton(url,username,password)
    new MysqlRepoManager(NamedDB(ConnectionPool.DEFAULT_NAME))
  }
  def createPostgres(url: String, username:String, password: String)= {
    ConnectionPool.singleton(url,username,password)
    new PostgresRepoManager(NamedDB(ConnectionPool.DEFAULT_NAME))
  }
}
class MysqlRepoManager(val db: NamedDB) extends JdbcRepoManager[MysqlDfsRepository] with MysqlSchemaSupport{
  def this(url: String, user:String, password: String) {
    this(NamedDB(ConnectionPool.DEFAULT_NAME))
    ConnectionPool.singleton(url,user,password)
  }
  override def tablePrefix: String = "t"

  override def openRepo(name: String): MysqlDfsRepository = {
    val builder = new MysqlRepoBuilder()
    builder.setDBName(db.name)
    builder.setRepoName(name)
    builder.build()
  }
}

class PostgresRepoManager(val db: NamedDB ) extends JdbcRepoManager[PostgresDfsRepository] with PostgresSchemaSupport{
  def this(url: String, user:String, password: String) {
    this(NamedDB(ConnectionPool.DEFAULT_NAME))
    ConnectionPool.singleton(url,user,password)
  }

  override def tablePrefix: String = "t"

  override def openRepo(name: String): PostgresDfsRepository = {
    val builder = new PostgresRepoBuilder()
    builder.setDBName(db.name)
    builder.setRepoName(name)
    builder.build()
  }
}
