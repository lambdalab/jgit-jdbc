package com.lambdalab.jgit.jdbc.schema

import com.lambdalab.jgit.jdbc.{DBConnection, JdbcDfsRepository}


trait MysqlSchemaSupport extends JdbcSchemaSupport {
  self: DBConnection with JdbcDfsRepository =>

  override def createPackTable = db autoCommit { implicit session =>
    val createTable =
      s"""CREATE TABLE IF NOT EXISTS `$packTableName` (
        `id` INT(11) UNSIGNED NOT NULL AUTO_INCREMENT,
         `source` varchar(255) DEFAULT NULL,
         `committed` tinyint(1) NOT NULL DEFAULT '0',
        PRIMARY KEY (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;"""
    session.execute(createTable)
    val createDataTable =
      s"""CREATE TABLE `${packTableName}_data` (
        `id` int(11) unsigned NOT NULL,
        `ext` varchar(8) NOT NULL DEFAULT '',
        `data` blob,
        PRIMARY KEY (`ext`,`id`),
        CONSTRAINT `fk_pack_id` FOREIGN KEY (`id`) REFERENCES `$packTableName` (`id`) ON DELETE CASCADE
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8;"""
    session.execute(createDataTable)
  }

  override def createRefTable = db autoCommit { implicit session =>
    val createTable =
      s"""CREATE TABLE IF NOT EXISTS `$refsTableName` (
           `name` VARCHAR(255) NOT NULL DEFAULT '',
           `object_id` varchar(40) DEFAULT NULL,
           `symbolic` tinyint(1) DEFAULT '0',
           `target` varchar(255) DEFAULT NULL
        PRIMARY KEY (`ref`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;"""

    session.execute(createTable)
  }

}
