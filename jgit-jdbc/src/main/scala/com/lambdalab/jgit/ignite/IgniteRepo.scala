package com.lambdalab.jgit.ignite

import javax.cache.processor.MutableEntry

import com.lambdalab.jgit.jdbc.ClearableRepo
import org.apache.ignite.cache.CacheEntryProcessor
import org.apache.ignite.{Ignite, Ignition}
import org.apache.ignite.configuration.{CollectionConfiguration, IgniteConfiguration}
import org.eclipse.jgit.internal.storage.dfs.{DfsObjDatabase, DfsRepository, DfsRepositoryBuilder, DfsRepositoryDescription}
import org.eclipse.jgit.lib.RefDatabase

class IgniteRepoBuilder extends DfsRepositoryBuilder[IgniteRepoBuilder, IgniteRepo] {
  def setRepoName(name: String) = {
    this.setRepositoryDescription(new DfsRepositoryDescription(name))
    this
  }

  var _ignite: Ignite = _

  def setIgnite(ignite: Ignite): IgniteRepoBuilder = {
    _ignite = ignite
    this
  }

  def setupIgnite(config: IgniteConfiguration => Unit): IgniteRepoBuilder = {
    val cfg = if (_ignite == null) {
      new IgniteConfiguration()
    } else {
      _ignite.configuration()
    }
    config(cfg)
    _ignite = Ignition.getOrStart(cfg)
    this
  }

  override def build() = {
    new IgniteRepo(this)
  }

}

class IgniteRepo(builder: IgniteRepoBuilder) extends DfsRepository(builder) with ClearableRepo with IgniteTxSupport {

  val ignite = builder._ignite
  lazy val objDb = new IgniteObjDB(this)
  lazy val refsDb = new IgniteRefDB(this)
  val descriptions = ignite.getOrCreateCache[String, String]("repo_descriptions")
  val sequences = ignite.getOrCreateCache[String,Long]("repo_seq")
  val repos = ignite.getOrCreateCache[String,Boolean]("repos")
  private val repoName: String = this.getDescription.getRepositoryName

  override def create(bare: Boolean): Unit = {
    super.create(bare)
    repos.put(repoName, true)
  }

  def incAndGetSeq(): Long = {
    sequences.invoke(repoName,new CacheEntryProcessor[String, Long, Long] {
      override def process(entry: MutableEntry[String, Long], arguments: AnyRef*): Long = {
        if(entry.exists()) {
          val value =entry.getValue
          entry.setValue(value+1)
          value
        } else {
          entry.setValue(1)
          0
        }
      }
    })
  }

  override def exists(): Boolean =  {
    repos.containsKey(repoName) && super.exists()
  }

  override def getObjectDatabase: DfsObjDatabase = objDb

  override def getRefDatabase: RefDatabase = refsDb

  override def clear(): Unit = {
    objDb.clear()
    refsDb.clear()
  }


  def delete() = {
    objDb.delete()
    refsDb.delete()
    descriptions.remove(repoName)
    repos.remove(repoName)
  }

  override def setGitwebDescription(description: String): Unit = {
    if (description != null)
      descriptions.put(repoName, description)
  }

  override def getGitwebDescription: String = {
    descriptions.get(repoName)
  }
}
