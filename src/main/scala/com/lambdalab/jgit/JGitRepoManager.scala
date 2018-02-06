package com.lambdalab.jgit

;

import org.eclipse.jgit.lib.Repository;

import java.util.List;

trait JGitRepoManager {
  def isRepoExists(name: String): Boolean

  def createRepo(name: String): Repository

  def openRepo(name: String): Repository

  def deleteRepo(name: String): Unit

  def allRepoNames(): java.util.Iterator[String]
}
