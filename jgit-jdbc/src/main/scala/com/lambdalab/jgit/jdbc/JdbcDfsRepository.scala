package com.lambdalab.jgit.jdbc

import java.io.IOException
import java.text.MessageFormat

import com.lambdalab.jgit.jdbc.schema.JdbcSchemaSupport
import org.eclipse.jgit.internal.JGitText
import org.eclipse.jgit.internal.storage.dfs.{DfsObjDatabase, DfsRepository, DfsRepositoryBuilder}
import org.eclipse.jgit.lib.{Constants, RefDatabase, RefUpdate, StoredConfig}
import org.eclipse.jgit.util.FS

abstract class JdbcDfsRepository(builder: DfsRepositoryBuilder[_ <: DfsRepositoryBuilder[_, _], _ <: DfsRepository])
    extends DfsRepository(builder) with JdbcSchemaSupport with ClearableRepo {

  private val repoName = this.getDescription.getRepositoryName

  def tablePrefix = "t"

  override def setGitwebDescription(description: String): Unit = {
    // for gerrit
  }
  private lazy val config = {
    val c = new JdbcRepoConfig(this)
    c.load()
    c
  }
  override def getConfig: StoredConfig = config

  private val objDatabase = new JdbcDfsObjDatabase(this)
  private val refDatabase = new JdbcDfsRefDatabase(this)

  override def getObjectDatabase: DfsObjDatabase = objDatabase

  override val getRefDatabase: RefDatabase = refDatabase

  lazy val fs = FS.detect()

  override def getFS: FS = {
    fs
  }

  override def exists(): Boolean = db localTxWithConnection {
    conn =>
      isTableExists(refsTableName, conn) && isTableExists(packTableName, conn) && (this.db readOnly {
        implicit s =>
          refDatabase.refs.all.nonEmpty
      })
  }

  override def create(bare: Boolean): Unit = {

    if (exists()) throw new IOException(MessageFormat.format(JGitText.get.repositoryAlreadyExists, ""))
    db localTxWithConnection { conn =>
      if (!isTableExists(refsTableName, conn)) {
        createRefTable(conn)
      }
      if (!isTableExists(packTableName, conn)) {
        createPackTable(conn)
      }
    }
    initRepo
  }

  private def initRepo = {
    val master = Constants.R_HEADS + Constants.MASTER
    val result = updateRef(Constants.HEAD, true).link(master)
    result match {
      case RefUpdate.Result.NEW | RefUpdate.Result.NO_CHANGE =>
      case _ => throw new IOException(result.name)
    }
  }

  def clear(): Unit = {
    refDatabase.clear()
    objDatabase.clear()
    config.clear()

  }
}