package com.lambdalab.jgit.ignite

import java.util

import com.lambdalab.jgit.streams.{ChunkedDfsOutputStream, ChunkedReadableChannel}
import io.netty.buffer.{ByteBuf, Unpooled}
import org.apache.ignite.Ignite
import org.apache.ignite.cache.query.ScanQuery
import org.apache.ignite.lang.IgniteBiPredicate
import org.eclipse.jgit.internal.storage.dfs.DfsObjDatabase.PackSource
import org.eclipse.jgit.internal.storage.dfs._
import org.eclipse.jgit.internal.storage.pack.PackExt

import scala.collection.JavaConverters._

case class Pack(id: Long,
                source: String,
                committed: Boolean,
                estimatedPackSize: Long) {
}

case class PackFile(id: Long,
                    ext: String,
                    var size: Long)

class IgniteObjDB(val repo: IgniteRepo) extends DfsObjDatabase(repo, new DfsReaderOptions) with IgniteTxSupport {


  override def ignite: Ignite = repo.ignite

  val chunkSize = 512 * 1024

  val seq = repo.ignite.atomicSequence(s"${repo.getDescription.getRepositoryName}_seq", 1, true)
  private  val packCacheName = s"${repo.getDescription.getRepositoryName}_packs"
  val packs = repo.ignite.getOrCreateCache[Long, Pack](packCacheName)
  val packFiles = repo.ignite.getOrCreateCache[String, PackFile](s"${packCacheName}_files")
  val packChunks = repo.ignite.getOrCreateCache[String, Array[Byte]](s"${packCacheName}_chunks")

  def clear() = {
    packChunks.clear()
    packFiles.clear()
    packs.clear()
  }

  def delete() = {
    packChunks.destroy()
    packFiles.destroy()
    packs.destroy()
  }

  override def openFile(desc: DfsPackDescription, packExt: PackExt): ReadableChannel = {
    val pack = desc.asInstanceOf[IgnitePackDescription].pack
    val ext = packExt.getExtension
    val file = packFiles.get(s"${pack.id}_$ext")
    new ChunkedReadableChannel(chunkSize) {
      private lazy val _size = {
        file.size
      }

      override def size(): Long = _size

      override def readChunk(chunk: Int): ByteBuf = {
        val key = s"${pack.id}_${ext}_$chunk"
        val bytes = packChunks.get(key)
        Unpooled.wrappedBuffer(bytes)
      }
    }
  }

  private def getFiles(id: Long) = {
    packFiles.query(
      new ScanQuery[String, PackFile](
        new IgniteBiPredicate[String, PackFile] {
          override def apply(key: String, value: PackFile): Boolean = key.startsWith(id.toString)
        })
    ).getAll.asScala.map(_.getValue).toSet
  }

  override def listPacks(): util.List[DfsPackDescription] = {
    packs.query(new ScanQuery[Long, Pack](new IgniteBiPredicate[Long, Pack] {
      override def apply(k: Long, v: Pack): Boolean = v.committed
    })).getAll.asScala.map {
      e =>
        val desc = new IgnitePackDescription(e.getValue).asInstanceOf[DfsPackDescription]
        getFiles(e.getKey).foreach{
          p =>
            val ext = PackExt.newPackExt(p.ext)
            desc.addFileExt(ext)
            desc.setFileSize(ext,p.size)
        }
        desc
    }.asJava
  }

  override def rollbackPack(collection: util.Collection[DfsPackDescription]): Unit = {
    val ids = collection.asScala.map(_.asInstanceOf[IgnitePackDescription].pack).map(_.id).toSet
    packs.clearAll(ids.asJava)
  }

  override def writeFile(dfsPackDescription: DfsPackDescription, packExt: PackExt): DfsOutputStream = {
    val pack = dfsPackDescription.asInstanceOf[IgnitePackDescription].pack

    val ext = packExt.getExtension
    val id = pack.id
    val fileKey = s"${id}_$ext"
    val packFile = packFiles.getAndPutIfAbsent(fileKey, new PackFile(id, ext, 0))



    new ChunkedDfsOutputStream(chunkSize) {
      override def readFromDB(chunk: Int): ByteBuf = {
        val key = s"${id}_${ext}_$chunk"
        val bytes = packChunks.get(key)
        Unpooled.wrappedBuffer(bytes)
      }

      override def flushBuffer(current: BufHolder): Unit = withTx { _ =>

        val buf = current.buf
        val key = s"${id}_${ext}_${current.chunk}"
        var bytes = packChunks.get(key)
        if (bytes == null) {
          bytes = new Array[Byte](buf.readableBytes())
          buf.readBytes(bytes)
          packChunks.put(key, bytes)
        } else if (buf.readableBytes() > 0) {
          val newBytes = new Array[Byte](bytes.length + buf.readableBytes())
          Array.copy(bytes, 0, newBytes, 0, bytes.length)
          buf.readBytes(newBytes, bytes.length, buf.readableBytes())
          packChunks.put(key, newBytes)
        }

        val f = packFiles.getAndPutIfAbsent(fileKey, PackFile(id, ext, 0))
        if (current.end > f.size) {
          f.size = current.end
        }
        packFiles.put(fileKey, f)
      }
    }
  }

  override def newPack(packSource: DfsObjDatabase.PackSource): DfsPackDescription = {
    val id = seq.incrementAndGet()
    val pack = new Pack(id, packSource.name(), false, 0)
    packs.put(id, pack)
    new IgnitePackDescription(pack)
  }

  override def commitPackImpl(desc: util.Collection[DfsPackDescription],
                              replaces: util.Collection[DfsPackDescription]): Unit = {
    val saves = desc.asScala.map(_.asInstanceOf[IgnitePackDescription]).toSet
    val deletes = if(replaces == null)  Nil.toSet.asJava else
      replaces.asScala.map(_.asInstanceOf[IgnitePackDescription].pack.id).toSet.asJava
    withTx {
      _ =>
        packs.clearAll(deletes)
        saves.foreach(p => {
          packs.replace(p.pack.id, Pack(p.pack.id, p.getPackSource.name(), true, p.getEstimatedPackSize))
        })
    }
  }

  class IgnitePackDescription(val pack: Pack) extends DfsPackDescription(repo.getDescription,
    s"pack-${pack.id}-${pack.source}") {

    setEstimatedPackSize(pack.estimatedPackSize)

    override def getPackSource: DfsObjDatabase.PackSource = {
      if (super.getPackSource == null) {
        this.setPackSource(PackSource.valueOf(pack.source))
      }
      super.getPackSource
    }
  }

}


