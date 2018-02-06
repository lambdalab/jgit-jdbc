package com.lambdalab.jgit.cassandra

import java.util
import java.util.UUID

import org.eclipse.jgit.internal.storage.dfs.DfsObjDatabase.PackSource
import org.eclipse.jgit.internal.storage.dfs._
import org.eclipse.jgit.internal.storage.pack.PackExt

import scala.collection.JavaConverters._

class CassandraObjDB(val repo: CassandraDfsRepo) extends DfsObjDatabase(repo, new DfsReaderOptions) {

  val packs = new CassandraPacks with CassandraContext {
    val settings = repo.cassandraSettings
  }

  val repoName = repo.getDescription.getRepositoryName

  override def openFile(desc: DfsPackDescription, ext: PackExt): ReadableChannel = {
    val id = desc.asInstanceOf[CassandraDfsPackDescription].id
    packs.readPack(repoName, id, ext.getExtension)
  }

  override def listPacks(): util.List[DfsPackDescription] = {
    val all = packs.allCommitted(repoName)
    new util.ArrayList(all.map(toDescription).asJava)
  }

  def toDescription(pack: Pack): DfsPackDescription = {
    val exts = packs.packDataExt(repoName, pack.id)
    val desc = CassandraDfsPackDescription(repo.getDescription, pack)
    exts.foreach {
      case (e, sz) =>
        val ext = PackExt.newPackExt(e)
        desc.addFileExt(ext)
        desc.setFileSize(ext, sz.longValue())
    }

    desc
  }

  override def rollbackPack(desc: util.Collection[DfsPackDescription]): Unit = {
    packs.batchDelete(repoName, toPacks(desc))
  }

  private def toPacks(desc: util.Collection[DfsPackDescription]) = {
    if (desc == null)
      Nil
    else {
      desc.asScala.map(_.asInstanceOf[CassandraDfsPackDescription])
    }
  }

  override def writeFile(desc: DfsPackDescription, ext: PackExt): DfsOutputStream = {
    val id = desc.asInstanceOf[CassandraDfsPackDescription].id
    packs.writePack(repoName, id, ext.getExtension)
  }

  override def newPack(source: DfsObjDatabase.PackSource): DfsPackDescription = {
    val p = packs.insertNew(repoName, source.name())
    toDescription(p)
  }

  override def commitPackImpl(desc: util.Collection[DfsPackDescription],
                              replaces: util.Collection[DfsPackDescription]): Unit = {
    packs.commitAll(repoName, toPacks(desc), toPacks(replaces))
  }

  def clear() = {
    packs.clear(repoName)
  }
}

case class CassandraDfsPackDescription(repoDescription: DfsRepositoryDescription, pack: Pack)
    extends DfsPackDescription(repoDescription, s"pack-${pack.id}-${pack.source}") {
  val id: UUID = pack.id
  setEstimatedPackSize(pack.estimatedPackSize)

  override def getPackSource: DfsObjDatabase.PackSource = {
    if (super.getPackSource == null) {
      this.setPackSource(PackSource.valueOf(pack.source))
    }
    super.getPackSource
  }
}