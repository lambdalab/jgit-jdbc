package com.lambdalab.jgit.jdbc

import com.lambdalab.jgit.jdbc.schema.{MysqlSchemaSupport, PostgresSchemaSupport}
import org.eclipse.jgit.internal.storage.dfs.{DfsRepository, DfsRepositoryBuilder, DfsRepositoryDescription}
import scalikejdbc.NamedDB

/**
  * Created by IntelliJ IDEA.
  * User: draco
  * Date: 2018/1/12
  * Time: 下午4:21
  */
trait JdbcDfsBuilder {
  self: DfsRepositoryBuilder[_,_] =>
  var dbName: Any = _
  def setDBName(name: Any): this.type  = {
    this.dbName = name
    this
  }
  def setRepoName(name: String): this.type = {
    this.setRepositoryDescription(new DfsRepositoryDescription(name))
    this
  }
}

class MysqlRepoBuilder extends DfsRepositoryBuilder[MysqlRepoBuilder, MysqlDfsRepository] with JdbcDfsBuilder{
  override def build() = new MysqlDfsRepository(this)
}

class MysqlDfsRepository(builder: MysqlRepoBuilder) extends JdbcDfsRepository(builder) with MysqlSchemaSupport {
  override def db: NamedDB = NamedDB(builder.dbName)
}

class PostgresRepoBuilder extends DfsRepositoryBuilder[PostgresRepoBuilder, PostgresDfsRepository]  with JdbcDfsBuilder {
  override def build() = new PostgresDfsRepository(this)
}

class PostgresDfsRepository(builder: PostgresRepoBuilder) extends JdbcDfsRepository(builder) with PostgresSchemaSupport {
  override def db: NamedDB = NamedDB(builder.dbName)
}


