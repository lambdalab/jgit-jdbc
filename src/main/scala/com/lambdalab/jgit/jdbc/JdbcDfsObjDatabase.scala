package com.lambdalab.jgit.jdbc

import java.util

import com.lambdalab.jgit.jdbc.io.{BlobDfsOutputStream, BlobReadableChannel, EmptyReadableChannel}
import com.lambdalab.jgit.jdbc.schema.{Pack, Packs}
import org.eclipse.jgit.internal.storage.dfs._
import org.eclipse.jgit.internal.storage.pack.PackExt
import scalikejdbc.NamedDB

import scala.collection.JavaConverters._

class JdbcDfsObjDatabase(val repo: JdbcDfsRepository with DBConnection)
    extends DfsObjDatabase(repo, new DfsReaderOptions) {
  def db: NamedDB = repo.db

  private val packs = new Packs(repo.packTableName, db)

  override def openFile(desc: DfsPackDescription, ext: PackExt): ReadableChannel = {
    val conn = db.autoClose(false)
    conn readOnly {
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
    val blob = conn.conn.createBlob()
    new BlobDfsOutputStream(blob) {
      override def commit(): Unit = {
        conn.localTx {
          implicit s =>
          packs.writeData(desc.asInstanceOf[JdbcDfsPackDescription].id, ext.getExtension, blob)
        }
        conn.close()
      }
    }
  }

  override def newPack(source: DfsObjDatabase.PackSource): DfsPackDescription = db localTx {
    implicit s =>
      toPackDescription(packs.insertNew(source.name()))
  }

  override def commitPackImpl(adds: util.Collection[DfsPackDescription],
                              replaces: util.Collection[DfsPackDescription]): Unit = db localTx {
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
  }

}
