package com.lambdalab.jgit.ignite

import java.util

import com.lambdalab.jgit.JGitRepoManager
import org.apache.ignite.{Ignite, Ignition}
import org.apache.ignite.configuration.{CollectionConfiguration, IgniteConfiguration}

class IgniteRepoManager(cfg: IgniteConfiguration) extends JGitRepoManager[IgniteRepo] with IgniteTxSupport {

  lazy val ignite: Ignite = Ignition.getOrStart(cfg)
  lazy val repos = ignite.set[String]("repos", new CollectionConfiguration().setCollocated(false))

  override def init(): Unit = {
    if(!ignite.active())
      ignite.active(true)
  }

  override def isRepoExists(name: String): Boolean = {
    repos.contains(name)
  }

  override def createRepo(name: String): IgniteRepo = withTx { _ =>
    repos.add(name)
    getRepo(name)
  }

  override def openRepo(name: String): IgniteRepo = withTx {
    _ =>
      if (isRepoExists(name)) {
        getRepo(name)
      } else {
        null
      }
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
      repo.clearRepo()
      repos.remove(name)
      repo.delete()
    }
  }

  override def allRepoNames(): util.Iterator[String] with AutoCloseable = {
    val it = repos.iterator()
    new util.Iterator[String] with AutoCloseable {
      override def next(): String = it.next()

      override def hasNext: Boolean = it.hasNext

      override def close(): Unit = repos.close()
    }
  }
}
