package com.lambdalab.jgit.jdbc

import java.util

import com.lambdalab.jgit.jdbc.io.{BlobDfsOutputStream, BlobReadableChannel, EmptyReadableChannel}
import com.lambdalab.jgit.jdbc.schema.{JdbcSchemaDelegate, JdbcSchemaSupport, Pack, Packs}
import org.eclipse.jgit.internal.storage.dfs.DfsObjDatabase.PackSource
import org.eclipse.jgit.internal.storage.dfs._
import org.eclipse.jgit.internal.storage.pack.PackExt
import scalikejdbc.NamedDB

import scala.collection.JavaConverters._

class JdbcDfsObjDatabase(val repo: JdbcDfsRepository with JdbcSchemaSupport)
    extends DfsObjDatabase(repo, new DfsReaderOptions) {

  def db: NamedDB = repo.db

  val packs = new Packs with JdbcSchemaDelegate {
    override def delegate = repo
  }

  override def openFile(desc: DfsPackDescription, ext: PackExt): ReadableChannel = {
    val conn = db.autoClose(false)
    conn.begin()
    conn withinTx  {
      implicit s =>
        packs.getData(desc.asInstanceOf[JdbcDfsPackDescription].id, ext.getExtension)
            .map(d => new BlobReadableChannel(d.data, conn)).getOrElse(EmptyReadableChannel)
    }
  }

  override def listPacks(): util.List[DfsPackDescription] = db localTx {
    implicit s =>
      return new util.ArrayList(packs.all.map(toPackDescription).asJava)
  }

  override def rollbackPack(desc: util.Collection[DfsPackDescription]): Unit = db localTx {
    implicit s =>
      packs.deleteAll(desc.asScala.map(_.asInstanceOf[JdbcDfsPackDescription].id).toSeq)
  }

  override def writeFile(desc: DfsPackDescription, ext: PackExt): DfsOutputStream = {
    val conn = db.autoClose(false)
    conn.begin()
    conn withinTx {
      implicit s =>
        packs.createBlobOutputStream(conn.conn, blob => {
          try {
            packs.writeData(desc.asInstanceOf[JdbcDfsPackDescription].id, ext.getExtension, blob)
            conn.commit()
          } finally
            conn.close()
        })
    }
  }

  override def newPack(source: DfsObjDatabase.PackSource): DfsPackDescription = db localTx {
    implicit s =>
      toPackDescription(packs.insertNew(source.name()))
  }

  override def commitPackImpl(adds: util.Collection[DfsPackDescription],
                              replaces: util.Collection[DfsPackDescription]): Unit

  = db localTx {
    implicit s =>
      packs.commitAll(adds.asScala.map(_.asInstanceOf[JdbcDfsPackDescription].pack).toSeq)
      if (replaces != null && !replaces.isEmpty)
        packs.deleteAll(replaces.asScala.map(_.asInstanceOf[JdbcDfsPackDescription].id).toSeq)
  }

  def toPackDescription(pack: Pack): DfsPackDescription = {
    JdbcDfsPackDescription(repo.getDescription, pack)
  }

  case class JdbcDfsPackDescription(repoDescription: DfsRepositoryDescription, pack: Pack)
      extends DfsPackDescription(repoDescription, s"pack-${pack.id}-${pack.source}") {
    val id: Long = pack.id

    override def getPackSource: DfsObjDatabase.PackSource = {
      if(super.getPackSource==null) {
        this.setPackSource(PackSource.valueOf(pack.source))
      }
      super.getPackSource
    }
  }

  def clear() = db localTx {
    implicit s =>
      packs.clearTable()
  }
}
