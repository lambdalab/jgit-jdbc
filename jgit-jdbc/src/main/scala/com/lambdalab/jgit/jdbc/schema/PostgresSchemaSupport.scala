package com.lambdalab.jgit.jdbc.schema

import java.sql.Connection

import scalikejdbc._

trait PostgresSchemaSupport extends JdbcSchemaSupport{

  lazy val packTableName = s"${tablePrefix}_packs"
  lazy val packDataTableName = s"${tablePrefix}_packs_data"
  lazy val packFileTableName = s"${tablePrefix}_packs_files"
  lazy val refsTableName = s"${tablePrefix}_refs"

  lazy val configsTableName: String = s"${tablePrefix}_configs"

  override def isTableExists(tableName: String,conn: Connection) = db localTx {
    implicit s =>
      sql"select * from pg_tables where tablename = ${tableName};"
              .map(rs => rs).single().apply().isDefined
  }


  override def createPackTable(conn: Connection) =  {
    conn.setReadOnly(false)
    val createTable =
      s"""CREATE TABLE IF NOT EXISTS "$packTableName" (
         repo varchar(255),
         id char(48) PRIMARY KEY,
         source varchar(255) DEFAULT NULL,
         committed boolean NOT NULL DEFAULT false,
         estimated_pack_size INT
        );
        CREATE INDEX "${packTableName}_repo_idx" ON "$packTableName"("repo");"""
    conn.prepareStatement(createTable).execute()
    val createFileTable =s"""
          CREATE TABLE $packFileTableName (
          id char(48) NOT NULL REFERENCES $packTableName ON DELETE CASCADE,
          ext varchar(8) NOT NULL DEFAULT '',
          size int NOT NULL DEFAULT '0',
          PRIMARY KEY (id,ext)
          )
      """
    conn.prepareStatement(createFileTable).execute()
    val createDataTable =
      s"""CREATE TABLE "${packTableName}_data" (
        id char(48),
        ext varchar(8) NOT NULL DEFAULT '',
        chunk int NOT NULL DEFAULT 0,
        data  bytea,
        PRIMARY KEY (id,ext, chunk),
         CONSTRAINT fk_${packFileTableName}_pack_id FOREIGN KEY (id,ext)
         REFERENCES $packFileTableName (id,ext) ON DELETE CASCADE
       )"""
    conn.prepareStatement(createDataTable).execute()
  }

  override def createRefTable(conn: Connection) = {
    val createTable =
      s"""CREATE TABLE IF NOT EXISTS "$refsTableName" (
            repo varchar(255) NOT NULL,
            name varchar(255) NOT NULL DEFAULT '',
            object_id varchar(40) DEFAULT NULL,
            symbolic boolean DEFAULT 'false',
            target varchar(255) DEFAULT NULL,
            PRIMARY KEY (repo, name)
      )"""
    conn.prepareStatement(createTable).execute()
    val createConfigsTable =
      s"""CREATE TABLE IF NOT EXISTS "$refsTableName" (
            repo varchar(255) NOT NULL,
            config text NOT NULL DEFAULT '',
            PRIMARY KEY (repo)
      )"""
    conn.prepareStatement(createConfigsTable).execute()
  }

  override def getUpsertSql(tableName: String,
                            columns: String,
                            columnBindings: String,
                            updates: String): String =s"""
      INSERT INTO $tableName ($columns) VALUES($columnBindings)
      ON CONFLICT ON CONSTRAINT ${tableName}_pkey  DO UPDATE set $updates
    """

  override val concatExpress: String = " data || ? "
}
