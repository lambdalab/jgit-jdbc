package com.lambdalab.jgit.jdbc.schema

import java.sql.{Blob, Connection, ResultSet}

import com.lambdalab.jgit.jdbc.io.BlobDfsOutputStream
import org.eclipse.jgit.internal.storage.dfs.DfsOutputStream

trait MysqlSchemaSupport extends JdbcSchemaSupport {

  lazy val packTableName = s"`${tablePrefix}_packs`"
  lazy val packDataTableName = s"`${tablePrefix}_packs_data`"
  lazy val refsTableName = s"`${tablePrefix}_refs`"

  override def createPackTable = db autoCommit { implicit session =>
    val createTable =
      s"""CREATE TABLE IF NOT EXISTS $packTableName (
        `id` INT(11) UNSIGNED NOT NULL AUTO_INCREMENT,
         `source` varchar(255) DEFAULT NULL,
         `committed` tinyint(1) NOT NULL DEFAULT '0',
        PRIMARY KEY (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;"""
    session.execute(createTable)
    val createDataTable =
      s"""CREATE TABLE $packDataTableName (
        `id` int(11) unsigned NOT NULL,
        `ext` varchar(8) NOT NULL DEFAULT '',
        `data` blob,
        PRIMARY KEY (`id`,`ext`),
        CONSTRAINT `fk_pack_id` FOREIGN KEY (`id`) REFERENCES $packTableName(`id`) ON DELETE CASCADE
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8;"""
    session.execute(createDataTable)
  }

  override def createRefTable = db autoCommit { implicit session =>
    val createTable =
      s"""CREATE TABLE IF NOT EXISTS $refsTableName (
           `name` VARCHAR(255) NOT NULL DEFAULT '' PRIMARY KEY,
           `object_id` varchar(40) DEFAULT NULL,
           `symbolic` tinyint(1) DEFAULT '0',
           `target` varchar(255) DEFAULT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;"""

    session.execute(createTable)
  }

  override def createBlobOutputStream(conn: Connection, commitCallback: (Any) => Unit): DfsOutputStream = {
    val blob = conn.createBlob()
    new BlobDfsOutputStream(blob) {
      override def commit(): Unit = commitCallback(blob)
    }
  }
  def createBlobFromRs(rs: ResultSet, columnLabel: String): Blob = {
      rs.getBlob(columnLabel)
  }

  override def getUpsertSql(tableName: String,
                            columns: String,
                            columnBindings: String,
                            updates: String): String =s"""
      insert into $tableName ($columns) values($columnBindings)
       on duplicate key update $updates
    """



}
