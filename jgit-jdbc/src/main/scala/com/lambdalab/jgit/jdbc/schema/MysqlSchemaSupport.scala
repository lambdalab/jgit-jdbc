package com.lambdalab.jgit.jdbc.schema

import java.sql.Connection

import scalikejdbc.ConnectionPool

trait MysqlSchemaSupport extends JdbcSchemaSupport {

  lazy val packTableName = s"`${tablePrefix}_packs`"
  lazy val packDataTableName = s"`${tablePrefix}_packs_data`"
  lazy val packFileTableName = s"`${tablePrefix}_packs_files`"
  lazy val refsTableName = s"`${tablePrefix}_refs`"
  lazy val configsTableName: String = s"`${tablePrefix}_configs`"

  private def unquote(tableName:String) = tableName.replaceAll("`","")
  override def createPackTable(conn: Connection) = {
    conn.setReadOnly(false)
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
    conn.prepareStatement(createTable).execute()
    val createFileTable =
      s"""
         CREATE TABLE $packFileTableName (
           `id` char(48) NOT NULL DEFAULT '',
           `ext` varchar(8) NOT NULL DEFAULT '',
           `size` int(11) unsigned NOT NULL DEFAULT '0',
           PRIMARY KEY (`id`,`ext`),
           CONSTRAINT `fk_${unquote(packTableName)}_pack_id` FOREIGN KEY (`id`) REFERENCES $packTableName (`id`) ON DELETE CASCADE
         ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
       """
    conn.prepareStatement(createFileTable).execute()
    val createDataTable =
      s"""CREATE TABLE $packDataTableName (
          `id` char(48) NOT NULL DEFAULT '',
          `ext` varchar(255) NOT NULL DEFAULT '',
          `chunk` int(11) NOT NULL DEFAULT '0',
          `data` longblob,
          PRIMARY KEY (`id`,`ext`,`chunk`),
          CONSTRAINT `fk_${unquote(packFileTableName)}_pack_id` FOREIGN KEY (`id`) REFERENCES $packFileTableName (`id`) ON DELETE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;"""
    conn.prepareStatement(createDataTable).execute()
  }

  override def createRefTable(conn: Connection) = {

      val createTable =
      s"""CREATE TABLE IF NOT EXISTS $refsTableName (
           `repo` VARCHAR(255) NOT NULL,
           `name` VARCHAR(255) NOT NULL,
           `object_id` varchar(40) DEFAULT NULL,
           `symbolic` tinyint(1) DEFAULT '0',
           `target` varchar(255) DEFAULT NULL,
           PRIMARY KEY(`repo`,`name`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;"""
    conn.setReadOnly(false)
    conn.prepareStatement(createTable).executeUpdate()
    val createConfigsTable =
      s"""CREATE TABLE IF NOT EXISTS $configsTableName (
           `repo` VARCHAR(255) NOT NULL,
           `config` TEXT NOT NULL,
           PRIMARY KEY(`repo`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;"""
    conn.setReadOnly(false)
    conn.prepareStatement(createConfigsTable).executeUpdate()
   }


  override def getUpsertSql(tableName: String,
                            columns: String,
                            columnBindings: String,
                            updates: String): String =s"""
      insert into $tableName ($columns) values($columnBindings)
       on duplicate key update $updates
    """

  override val concatExpress = "concat(data, ?)"

}
