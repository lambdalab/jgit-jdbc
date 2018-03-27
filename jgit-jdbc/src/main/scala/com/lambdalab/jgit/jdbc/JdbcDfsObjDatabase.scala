package com.lambdalab.jgit.jdbc

import java.io.InputStream
import java.util

import com.lambdalab.jgit.jdbc.schema.{JdbcSchemaDelegate, JdbcSchemaSupport, Pack, Packs}
import com.lambdalab.jgit.streams._
import io.netty.buffer.{ByteBuf, Unpooled}
import org.eclipse.jgit.internal.storage.dfs.DfsObjDatabase.PackSource
import org.eclipse.jgit.internal.storage.dfs._
import org.eclipse.jgit.internal.storage.pack.PackExt
import scalikejdbc.{DBSession, NamedDB}

import scala.collection.JavaConverters._

class JdbcDfsObjDatabase(val repo: JdbcDfsRepository with JdbcSchemaSupport)
    extends DfsObjDatabase(repo, new DfsReaderOptions) with LocalFileCacheSupport {

  def db: NamedDB = repo.db

  val packs = new Packs with JdbcSchemaDelegate {
    override def delegate = repo

    override val repoName = repo.getDescription.getRepositoryName
  }

  val chunkSize = 512 * 1024 // 512K Chunk Size

  override def openFile(desc: DfsPackDescription, packExt: PackExt): ReadableChannel = {
    val id = desc.asInstanceOf[JdbcDfsPackDescription].id
    val ext = packExt.getExtension
    val fileOption = db readOnly {
      implicit s =>
        packs.getFile(id, ext)
    }
    fileOption.map {
      f =>
        new ChunkedReadableChannel(chunkSize, getFileCache(id, ext)) {
          override def size(): Long = f.size
        }
    }.getOrElse(EmptyReadableChannel)
  }

  private def readStream(is: InputStream) = {
    val buf = Unpooled.buffer(chunkSize)
    buf.writeBytes(is, chunkSize)
    is.close()
    buf
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

    val id = desc.asInstanceOf[JdbcDfsPackDescription].id
    val ext = packExt.getExtension

    db localTx {
      implicit s =>
        packs.initFile(id, ext)
    }
    val os = new ChunkedDfsOutputStream(chunkSize, getFileCache(id, ext)) {
      override def flushBuffer(current: BufHolder): Unit = {
        db localTx {
          implicit s =>
            packs.writeData(id, ext, current.chunk, current.buf)
            packs.updateFileSize(id, ext, current.end)
        }
      }
    }
    os
  }

  override def newPack(source: DfsObjDatabase.PackSource): DfsPackDescription = db localTx {
    implicit s =>
      toPackDescription(packs.insertNew(source.name()))
  }

  override def commitPackImpl(adds: util.Collection[DfsPackDescription],
                              replaces: util.Collection[DfsPackDescription]): Unit = db localTx {
    implicit s =>
      packs.commitAll(adds.asScala.map(_.asInstanceOf[JdbcDfsPackDescription]).toSeq)
      if (replaces != null && !replaces.isEmpty)
        packs.deleteAll(replaces.asScala.map(_.asInstanceOf[JdbcDfsPackDescription].id).toSeq)
  }

  def toPackDescription(pack: Pack)(implicit dBSession: DBSession): DfsPackDescription = {
    val exts = packs.getFiles(pack.id)
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

  override def loadChunk(id: String, ext: String, chunk: Int): ByteBuf = {
    db readOnly {
      implicit s =>
        val data = packs.getData(id, ext, chunk)
        data.map(p => readStream(p.data)).getOrElse(Unpooled.EMPTY_BUFFER)
    }
  }

  override def close(): Unit = {
    super.close()
    closeCaches()
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