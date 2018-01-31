package com.lambdalab.jgit.jdbc

import java.io.IOException
import java.text.MessageFormat

import com.lambdalab.jgit.jdbc.schema.JdbcSchemaSupport
import org.eclipse.jgit.internal.JGitText
import org.eclipse.jgit.internal.storage.dfs.{DfsObjDatabase, DfsRepository, DfsRepositoryBuilder}
import org.eclipse.jgit.lib.{Constants, RefDatabase, RefUpdate}

abstract class JdbcDfsRepository(builder: DfsRepositoryBuilder[_ <: DfsRepositoryBuilder[_,_], _ <: DfsRepository])
    extends DfsRepository(builder) with JdbcSchemaSupport with ClearableRepo{

  def tablePrefix = this.getDescription.getRepositoryName

  private val objDatabase = new JdbcDfsObjDatabase(this)
  private val refDatabase = new JdbcDfsRefDatabase(this)

  override def getObjectDatabase: DfsObjDatabase = objDatabase

  override val getRefDatabase: RefDatabase = refDatabase

  override def exists(): Boolean = {
    isTableExists(refsTableName) && isTableExists(packTableName)
  }

  override def create(bare: Boolean): Unit = {
    if (exists()) throw new IOException(MessageFormat.format(JGitText.get.repositoryAlreadyExists, ""))
    if (!isTableExists(refsTableName)) {
      createRefTable
    }
    if (!isTableExists(packTableName)) {
      createPackTable
    }
    val master = Constants.R_HEADS + Constants.MASTER
    val result = updateRef(Constants.HEAD, true).link(master)
    if (result ne RefUpdate.Result.NEW) throw new IOException(result.name)
  }

  def clearRepo(): Unit = {
    refDatabase.clear()
    objDatabase.clear()
  }
}