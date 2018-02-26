package com.lambdalab.jgit.ignite

import com.lambdalab.jgit.jdbc.ClearableRepo
import org.apache.ignite.{Ignite, Ignition}
import org.apache.ignite.configuration.IgniteConfiguration
import org.eclipse.jgit.internal.storage.dfs.{DfsObjDatabase, DfsRepository, DfsRepositoryBuilder, DfsRepositoryDescription}
import org.eclipse.jgit.lib.RefDatabase


class IgniteRepoBuilder extends DfsRepositoryBuilder[IgniteRepoBuilder,IgniteRepo] {
  def setRepoName(name: String) = {
    this.setRepositoryDescription(new DfsRepositoryDescription(name))
    this
  }

  var _ignite: Ignite = _

  def setIgnite(ignite: Ignite) : IgniteRepoBuilder = {
    _ignite = ignite
    this
  }

  def setupIgnite(config : IgniteConfiguration => Unit): IgniteRepoBuilder = {
    val cfg = if(_ignite ==null) {
      new IgniteConfiguration()
    } else {
      _ignite.configuration()
    }
    config(cfg)
    _ignite = Ignition.getOrStart(cfg)
    this
  }

  override def build() ={
    new IgniteRepo(this)
  }


}

class IgniteRepo(builder: IgniteRepoBuilder) extends DfsRepository(builder) with ClearableRepo{


  lazy val ignite =  builder._ignite
  lazy val objDb =  new IgniteObjDB(this)
  lazy val refsDb = new IgniteRefDB(this)
  override def getObjectDatabase: DfsObjDatabase =  objDb

  override def getRefDatabase: RefDatabase = refsDb

  override def clear(): Unit = {
    objDb.clear()
    refsDb.clear()
  }
  def delete() = {
    objDb.delete()
    refsDb.delete()
  }
}
