package com.lambdalab.jgit.jdbc.schema

import java.sql.{Blob, Connection, ResultSet}

import com.lambdalab.jgit.jdbc.steams.BlobDfsOutputStream
import org.eclipse.jgit.internal.storage.dfs.DfsOutputStream

trait MysqlSchemaSupport extends JdbcSchemaSupport {

  lazy val packTableName = s"`${tablePrefix}_packs`"
  lazy val packDataTableName = s"`${tablePrefix}_packs_data`"
  lazy val packFileTableName = s"`${tablePrefix}_packs_files`"
  lazy val refsTableName = s"`${tablePrefix}_refs`"

  private def unquote(tableName:String) = tableName.replaceAll("`","")
  override def createPackTable = db autoCommit { implicit session =>
    val createTable =
      s"""CREATE TABLE IF NOT EXISTS $packTableName (
         `repo` varchar(255) NOT NULL DEFAULT '',
         `id` char(48) NOT NULL DEFAULT '',
         `source` varchar(255) DEFAULT NULL,
         `committed` tinyint(1) NOT NULL DEFAULT '0',
         `estimated_pack_size` INT(11) UNSIGNED,
         PRIMARY KEY (`id`),
         KEY `repo` (`repo`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;"""
    session.execute(createTable)
    val createFileTable =
      s"""
         CREATE TABLE $packFileTableName (
           `id` char(48) NOT NULL DEFAULT '',
           `ext` varchar(8) NOT NULL DEFAULT '',
           `size` int(11) unsigned NOT NULL DEFAULT '0'
           PRIMARY KEY (`id`,`ext`),
           CONSTRAINT `fk_${unquote(packTableName)}_pack_id` FOREIGN KEY (`id`) REFERENCES $packTableName (`id`) ON DELETE CASCADE
         ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
       """
    session.execute(createFileTable)
    val createDataTable =
      s"""CREATE TABLE $packDataTableName (
          `id` char(48) NOT NULL DEFAULT '',
          `ext` varchar(255) NOT NULL DEFAULT '',
          `chunk` int(11) NOT NULL DEFAULT '0',
          `data` longblob,
          PRIMARY KEY (`id`,`ext`,`chunk`),
          CONSTRAINT `fk_${unquote(packFileTableName)}_pack_id` FOREIGN KEY (`id`) REFERENCES $packFileTableName (`id`) ON DELETE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;"""
    session.execute(createDataTable)
  }

  override def createRefTable = db autoCommit { implicit session =>
    val createTable =
      s"""CREATE TABLE IF NOT EXISTS $refsTableName (
           `repo` VARCHAR(255) NOT NULL,
           `name` VARCHAR(255) NOT NULL,
           `object_id` varchar(40) DEFAULT NULL,
           `symbolic` tinyint(1) DEFAULT '0',
           `target` varchar(255) DEFAULT NULL,
           PRIMARY KEY(`repo`,`name`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;"""

    session.execute(createTable)
  }

  override def createBlobOutputStream(conn: Connection, commitCallback: (Any) => Unit): DfsOutputStream = {
    val blob = conn.createBlob()
    new BlobDfsOutputStream(blob) {
      override def commit(): Unit = commitCallback(blob)

      override def close(): Unit = {
        super.close()
        conn.close()
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
      insert into $tableName ($columns) values($columnBindings)
       on duplicate key update $updates
    """



}
