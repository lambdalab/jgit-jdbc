package com.lambdalab.jgit.jdbc.schema

import java.sql.{Blob, Connection, ResultSet}

import org.eclipse.jgit.internal.storage.dfs.DfsOutputStream
import scalikejdbc._

trait JdbcSchemaSupport {
  def db: NamedDB

  def createPackTable: Unit

  def createRefTable: Unit

  def isTableExists(tableName: String) = db.getTable(tableName).isDefined

  def tablePrefix: String

  def packTableName: String

  def packDataTableName: String

  def packFileTableName: String

  def refsTableName: String

  def getUpsertSql(tableName: String,
                   columns: String,
                   columnBindings: String,
                   updates: String): String

  def concatExpress: String
}

trait JdbcSchemaDelegate extends JdbcSchemaSupport {
  def delegate: JdbcSchemaSupport

  def db: NamedDB = delegate.db

  def createPackTable: Unit = delegate.createPackTable

  def createRefTable: Unit = delegate.createRefTable

  def tablePrefix: String = delegate.tablePrefix

  def packTableName: String = delegate.packTableName

  def packDataTableName: String = delegate.packDataTableName

  def packFileTableName: String = delegate.packFileTableName


  def refsTableName: String = delegate.refsTableName


  def getUpsertSql(tableName: String,
                   columns: String,
                   columnBindings: String,
                   updates: String): String = delegate.getUpsertSql(tableName, columns, columnBindings, updates)
  def concatExpress = delegate.concatExpress
}