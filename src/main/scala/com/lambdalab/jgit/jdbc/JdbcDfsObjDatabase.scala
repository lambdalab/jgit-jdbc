package com.lambdalab.jgit.jdbc

import java.nio.ByteBuffer
import java.util

import com.lambdalab.jgit.jdbc.schema.{JdbcSchemaDelegate, JdbcSchemaSupport, Pack, Packs}
import com.lambdalab.jgit.streams.{ChunkedDfsOutputStream, ChunkedReadableChannel}
import org.eclipse.jgit.internal.storage.dfs.DfsObjDatabase.PackSource
import org.eclipse.jgit.internal.storage.dfs._
import org.eclipse.jgit.internal.storage.pack.PackExt
import scalikejdbc.{ConnectionPool, DB, DBSession, NamedDB}
import io.netty.buffer.{ByteBuf, EmptyByteBuf, Unpooled}

import scala.collection.JavaConverters._

class JdbcDfsObjDatabase(val repo: JdbcDfsRepository with JdbcSchemaSupport)
    extends DfsObjDatabase(repo, new DfsReaderOptions) {

  def db: NamedDB = repo.db

  val packs = new Packs with JdbcSchemaDelegate {
    override def delegate = repo

    override val repoName = repo.getDescription.getRepositoryName
  }

  val chunkSize = 1024 * 1024 // 1M Chunk Size

  override def openFile(desc: DfsPackDescription, packExt: PackExt): ReadableChannel = {
    val conn = db.autoClose(false)
    val id = desc.asInstanceOf[JdbcDfsPackDescription].id
    val ext = packExt.getExtension
    conn.begin()
    conn withinTx {
      implicit s =>
        new ChunkedReadableChannel(chunkSize) {
          override def readChunk(chunk: Int): ByteBuf = {
            val chunk = packs.getData(id, ext, chunk)
            chunk.map(_.buf).getOrElse(EmptyByteBuf)
          }

          lazy val _size = {
            packs.getFile(id, ext).map(_.size).getOrElse(0)
          }

          override def size(): Long = _size
        }
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

  override def writeFile(desc: DfsPackDescription, packExt: PackExt): DfsOutputStream = {
    val c = ConnectionPool.borrow(db.name)
    val conn = DB(c)
    val id = desc.asInstanceOf[JdbcDfsPackDescription].id
    val ext = packExt.getExtension
    conn.begin()
    conn withinTx {
      implicit s =>
        new ChunkedDfsOutputStream(chunkSize) {
          override def readFromDB(chunk: Int): ByteBuf = {
            packs.getData(id, ext, chunk).map(_.buf).getOrElse(EmptyByteBuf)
          }

          override def flushBuffer(current: BufHolder): Unit = {
            packs.writeData(id, ext, current.chunk, current.buf.array())
          }
        }
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
      packs.commitAll(adds.asScala.map(_.asInstanceOf[JdbcDfsPackDescription]).toSeq)
      if (replaces != null && !replaces.isEmpty)
        packs.deleteAll(replaces.asScala.map(_.asInstanceOf[JdbcDfsPackDescription].id).toSeq)
  }

  def toPackDescription(pack: Pack)(implicit dBSession: DBSession): DfsPackDescription = {
    val exts = packs.dataExts(pack.id)
    val desc = JdbcDfsPackDescription(repo.getDescription, pack)
    exts.foreach {
      f =>
        val ext = PackExt.newPackExt(f.ext)
        desc.addFileExt(ext)
        desc.setFileSize(ext, f.size)
    }
    desc
  }

  def clear() = db localTx {
    implicit s =>
      packs.clearTable()
  }
}

case class JdbcDfsPackDescription(repoDescription: DfsRepositoryDescription, pack: Pack)
    extends DfsPackDescription(repoDescription, s"pack-${pack.id}-${pack.source}") {
  val id: String = pack.id
  setEstimatedPackSize(pack.estimatedPackSize)

  override def getPackSource: DfsObjDatabase.PackSource = {
    if (super.getPackSource == null) {
      this.setPackSource(PackSource.valueOf(pack.source))
    }
    super.getPackSource
  }
}