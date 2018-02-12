package com.lambdalab.jgit

;

import org.eclipse.jgit.lib.Repository;

import java.util.List;

trait JGitRepoManager[T <: Repository] {
  def init(): Unit

  def isRepoExists(name: String): Boolean

  def createRepo(name: String): T

  def openRepo(name: String): T

  def deleteRepo(name: String): Unit

  def allRepoNames(): java.util.Iterator[String] with AutoCloseable
}
