package com.lambdalab.jgit.jdbc

import com.lambdalab.jgit.jdbc.schema.MysqlSchemaSupport
import org.eclipse.jgit.internal.storage.dfs.{DfsRepositoryBuilder, DfsRepositoryDescription}
import scalikejdbc.NamedDB

/**
  * Created by IntelliJ IDEA.
  * User: draco
  * Date: 2018/1/12
  * Time: 下午4:21
  */
abstract class JdbcDfsBuilder extends DfsRepositoryBuilder[JdbcDfsBuilder, JdbcDfsRepository] {

}

object JdbcDfsBuilder {
  def createMysqlBuilder(repoName: String, dbName: Any) = {
    val builder = new JdbcDfsBuilder() {
      override def build() = {
        new JdbcDfsRepository(this) with DBConnection with MysqlSchemaSupport{
          override def db: NamedDB = NamedDB(dbName)
        }
      }
    }
    builder.setRepositoryDescription(new DfsRepositoryDescription(repoName))
    builder
  }
}
