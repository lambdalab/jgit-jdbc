package com.lambdalab.jgit.ignite

import java.util

import com.lambdalab.jgit.JGitRepoManager
import org.apache.ignite.{Ignite, Ignition}
import org.apache.ignite.configuration.{CollectionConfiguration, IgniteConfiguration}

class IgniteRepoManager(cfg: IgniteConfiguration) extends JGitRepoManager[IgniteRepo] {

  lazy val ignite: Ignite = Ignition.getOrStart(cfg)
  lazy val repos = ignite.getOrCreateCache[String,Boolean]("repos")

  override def init(): Unit = {
    if(!ignite.active())
      ignite.active(true)
  }

  override def isRepoExists(name: String): Boolean = {
    getRepo(name).exists()
  }

  override def createRepo(name: String): IgniteRepo = {
    val repo = getRepo(name)
    repo.create(false)
    repo
  }

  override def openRepo(name: String): IgniteRepo =  {
    getRepo(name)
  }

  private def getRepo(name: String) = {
    val builder = new IgniteRepoBuilder()
    builder.setIgnite(ignite)
    builder.setRepoName(name)
    builder.build()
  }

  override def deleteRepo(name: String): Unit = {
    if (isRepoExists(name)) {
      val repo = getRepo(name)
      repo.delete()
    }
  }

  override def allRepoNames(): util.Iterator[String] with AutoCloseable = {
    val it = repos.iterator()
    new util.Iterator[String] with AutoCloseable {
      override def next(): String = it.next().getKey

      override def hasNext: Boolean = it.hasNext

      override def close(): Unit = {}
    }
  }
}
