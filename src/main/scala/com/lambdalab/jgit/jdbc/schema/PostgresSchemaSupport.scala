package com.lambdalab.jgit.jdbc.schema

import java.sql.{Blob, Connection, ResultSet}

import com.lambdalab.jgit.jdbc.io.LargeObjectDfsOutputStream
import org.eclipse.jgit.internal.storage.dfs.DfsOutputStream
import org.postgresql.PGConnection
import org.postgresql.largeobject.LargeObjectManager
import scalikejdbc._

trait PostgresSchemaSupport extends JdbcSchemaSupport{

  lazy val packTableName = s"${tablePrefix}_packs"
  lazy val packDataTableName = s"${tablePrefix}_packs_data"
  lazy val refsTableName = s"${tablePrefix}_refs"

  override def isTableExists(tableName: String) = db localTx {
    implicit s =>
      sql"select * from pg_tables where tablename = ${tableName};"
              .map(rs => rs).single().apply().isDefined
  }


  override def createPackTable = db autoCommit { implicit session =>
    val createTable =
      s"""CREATE TABLE IF NOT EXISTS "$packTableName" (
         id SERIAL PRIMARY KEY,
         source varchar(255) DEFAULT NULL,
         committed boolean NOT NULL DEFAULT false
        )"""
    session.execute(createTable)
    val createDataTable =
      s"""CREATE TABLE "${packTableName}_data" (
        id integer REFERENCES test_packs ON DELETE CASCADE,
        ext varchar(8) NOT NULL DEFAULT '',
        data oid,
        PRIMARY KEY (id,ext)
      )"""
    session.execute(createDataTable)
  }

  override def createRefTable = db autoCommit { implicit session =>
    val createTable =
      s"""CREATE TABLE IF NOT EXISTS "$refsTableName" (
             name varchar(255) NOT NULL DEFAULT '',
            object_id varchar(40) DEFAULT NULL,
            symbolic boolean DEFAULT 'false',
            target varchar(255) DEFAULT NULL,
            PRIMARY KEY (name)
      )"""
    session.execute(createTable)
  }

  override def createBlobOutputStream(conn: Connection, commitCallback: Any => Unit): DfsOutputStream = {
    val  pgConn = conn.unwrap(classOf[org.postgresql.jdbc4.Jdbc4Connection])
    pgConn.setAutoCommit(false)
    val lobj = pgConn.getLargeObjectAPI
    val oid = lobj.createLO(LargeObjectManager.READ | LargeObjectManager.WRITE)
    val obj = lobj.open(oid, LargeObjectManager.WRITE)
    new LargeObjectDfsOutputStream(obj) {
      override def commit(): Unit = {
        commitCallback(oid)
      }
    }
  }

  def createBlobFromRs(rs: ResultSet, columnLabel: String): Blob = {
     rs.getBlob(columnLabel)
  }

  override def getUpsertSql(tableName: String,
                            columns: String,
                            columnBindings: String,
                            updates: String): String =s"""
      INSERT INTO $tableName ($columns) VALUES($columnBindings)
      ON CONFLICT ON CONSTRAINT ${tableName}_pkey  DO UPDATE set $updates
    """
}
