package com.lambdalab.jgit.jdbc

import com.lambdalab.jgit.jdbc.schema.{Configs, JdbcSchemaDelegate, JdbcSchemaSupport}
import org.eclipse.jgit.lib.StoredConfig
import scalikejdbc.NamedDB

class JdbcRepoConfig(val repo: JdbcDfsRepository with JdbcSchemaSupport) extends StoredConfig {
  def db: NamedDB = repo.db

  val configs = new Configs with JdbcSchemaDelegate {
    override def delegate = repo

    override val repoName = repo.getDescription.getRepositoryName
  }

  override def save(): Unit = db localTx {
    implicit s =>
      configs.saveText(this.toText)
  }

  override def load(): Unit = {
    db.readOnly {
      implicit s =>
        configs.loadText.foreach(fromText)
    }
  }

  override def clear(): Unit = db localTx {
    implicit s =>
      super.clear()
      configs.clear
  }
}
