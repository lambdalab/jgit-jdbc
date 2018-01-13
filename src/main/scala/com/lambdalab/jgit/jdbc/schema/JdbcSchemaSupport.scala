package com.lambdalab.jgit.jdbc.schema

import com.lambdalab.jgit.jdbc.{DBConnection, JdbcDfsRepository}
import scalikejdbc._

trait JdbcSchemaSupport {
  self: DBConnection with JdbcDfsRepository =>

  def createPackTable: Unit

  def createRefTable: Unit

  def isTableExists(tableName: String) = db.getTable(tableName).isDefined

  val packTableName = s"${tablePrefix}_packs"
  val refsTableName = s"${tablePrefix}_refs"
}

