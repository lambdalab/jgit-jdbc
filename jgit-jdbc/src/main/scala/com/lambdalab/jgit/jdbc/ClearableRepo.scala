package com.lambdalab.jgit.jdbc

import java.io.IOException

import org.eclipse.jgit.internal.storage.dfs.DfsRepository
import org.eclipse.jgit.lib.{Constants, RefUpdate}

trait ClearableRepo {
  self: DfsRepository=>
  protected def clear(): Unit

  def clearRepo(init: Boolean = true): Unit = {
    clear()
    scanForRepoChanges()
    if (init) {
      initRepo
    }
  }

  private def initRepo = {
    val master = Constants.R_HEADS + Constants.MASTER
    val result = updateRef(Constants.HEAD, true).link(master)
    result match {
      case RefUpdate.Result.NEW | RefUpdate.Result.NO_CHANGE =>
      case _ => throw new IOException(result.name)
    }
  }
}