package com.lambdalab.jgit.jdbc.schema

import scalikejdbc._

trait PostgresSchemaSupport extends JdbcSchemaSupport{

  lazy val packTableName = s"${tablePrefix}_packs"
  lazy val packDataTableName = s"${tablePrefix}_packs_data"
  lazy val packFileTableName = s"${tablePrefix}_packs_files"
  lazy val refsTableName = s"${tablePrefix}_refs"

  override def isTableExists(tableName: String) = db localTx {
    implicit s =>
      sql"select * from pg_tables where tablename = ${tableName};"
              .map(rs => rs).single().apply().isDefined
  }


  override def createPackTable = db autoCommit { implicit session =>
    val createTable =
      s"""CREATE TABLE IF NOT EXISTS "$packTableName" (
         repo varchar(255),
         id char(48) PRIMARY KEY,
         source varchar(255) DEFAULT NULL,
         committed boolean NOT NULL DEFAULT false,
         estimated_pack_size INT
        );
        CREATE INDEX "${packTableName}_repo_idx" ON "$packTableName"("repo");"""
    session.execute(createTable)
    val createFileTable =s"""
          CREATE TABLE $packFileTableName (
          id char(48) NOT NULL REFERENCES $packTableName ON DELETE CASCADE,
          ext varchar(8) NOT NULL DEFAULT '',
          size int NOT NULL DEFAULT '0',
          PRIMARY KEY (id,ext)
          )
      """
    session.execute(createFileTable)
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
    session.execute(createDataTable)
  }

  override def createRefTable = db autoCommit { implicit session =>
    val createTable =
      s"""CREATE TABLE IF NOT EXISTS "$refsTableName" (
            repo varchar(255) NOT NULL,
            name varchar(255) NOT NULL DEFAULT '',
            object_id varchar(40) DEFAULT NULL,
            symbolic boolean DEFAULT 'false',
            target varchar(255) DEFAULT NULL,
            PRIMARY KEY (repo, name)
      )"""
    session.execute(createTable)
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
