package com.lambdalab.jgit.jdbc

import java.util

import com.lambdalab.jgit.JGitRepoManager
import org.eclipse.jgit.lib.Repository

class JdbcRepoManager extends JGitRepoManager{

  override def isRepoExists(name: String): Boolean = ???

  override def createRepo(name: String): Repository = ???

  override def openRepo(name: String): Repository = ???

  override def deleteRepo(name: String): Unit = ???

  override def allRepoNames(): util.Iterator[String] = ???
}
