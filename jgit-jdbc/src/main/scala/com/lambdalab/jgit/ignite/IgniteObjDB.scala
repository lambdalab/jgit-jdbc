package com.lambdalab.jgit.ignite

import java.util
import java.util.function.Consumer

import javax.cache.Cache
import com.lambdalab.jgit.streams.{ChunkedDfsOutputStream, ChunkedReadableChannel, LocalFileCacheSupport}
import io.netty.buffer.{ByteBuf, Unpooled}
import org.apache.ignite.binary.BinaryObject
import org.apache.ignite.cache.query.ScanQuery
import org.apache.ignite.lang.IgniteBiPredicate
import org.apache.ignite.{Ignite, IgniteCache}
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

class IgniteObjDB(val repo: IgniteRepo) extends DfsObjDatabase(repo, new DfsReaderOptions) with IgniteTxSupport with LocalFileCacheSupport{

  override def ignite: Ignite = repo.ignite

  val chunkSize = 512 * 1024

  private val packCacheName = s"${repo.getDescription.getRepositoryName}_packs"
  val packs: IgniteCache[Long, BinaryObject] =
    repo.ignite.getOrCreateCache(packCacheName).withKeepBinary()
  val packFiles:IgniteCache[String, BinaryObject] =
    repo.ignite.getOrCreateCache[String, BinaryObject](s"${packCacheName}_files").withKeepBinary()
  val packChunks = repo.ignite.getOrCreateCache[String, Array[Byte]](s"${packCacheName}_chunks")

  implicit def bin2Pack(bin: BinaryObject): Pack = {
    Pack(bin.field("id"), bin.field("source"), bin.field("committed"), bin.field("estimatedPackSize"))
  }

  implicit def Pack2bin(pack: Pack): BinaryObject = {
    ignite.binary().toBinary(pack)
  }

  implicit def bin2File(bin: BinaryObject): PackFile = {
    PackFile(bin.field("id"), bin.field("ext"), bin.field("size"))
  }

  implicit def file2bin(f: PackFile): BinaryObject = {
    ignite.binary().toBinary(f)
  }

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
    new ChunkedReadableChannel(chunkSize, getFileCache(pack.id.toString,ext)) {
      private lazy val _size = {
        file.size
      }
      override def size(): Long = _size
    }
  }

  private def getFiles(id: Long) = {
    packFiles.query(
      new ScanQuery[String, BinaryObject](
        new IgniteBiPredicate[String, BinaryObject] {
          override def apply(key: String, value: BinaryObject): Boolean = key.startsWith(id.toString)
        })
    ).getAll.asScala.map(_.getValue).toSet
  }

  override def listPacks(): util.List[DfsPackDescription] = {
    packs.query(new ScanQuery[Long, BinaryObject](new IgniteBiPredicate[Long, BinaryObject] {
      override def apply(k: Long, v: BinaryObject): Boolean = v.field("committed")
    })).getAll.asScala.map {
      e =>
        val desc = new IgnitePackDescription(e.getValue).asInstanceOf[DfsPackDescription]
        getFiles(e.getKey).foreach {
          p =>
            val ext = PackExt.newPackExt(p.ext)
            desc.addFileExt(ext)
            desc.setFileSize(ext, p.size)
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

    new ChunkedDfsOutputStream(chunkSize, getFileCache(id.toString, ext)) {

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

        val f: PackFile = packFiles.getAndPutIfAbsent(fileKey, PackFile(id, ext, 0))
        if (current.end > f.size) {
          f.size = current.end
        }
        packFiles.put(fileKey, f)
      }
    }
  }

  override def newPack(packSource: DfsObjDatabase.PackSource): DfsPackDescription = {
    val id = repo.incAndGetSeq()
    val pack = new Pack(id, packSource.name(), false, 0)
    packs.put(id, pack)
    new IgnitePackDescription(pack)
  }

  override def commitPackImpl(desc: util.Collection[DfsPackDescription],
                              replaces: util.Collection[DfsPackDescription]): Unit = {
    val saves = desc.asScala.map(_.asInstanceOf[IgnitePackDescription]).toSet
    val deletes= new util.HashSet[Long]()
    if(replaces!=null) {
      replaces.asScala.foreach(r => deletes.add(r.asInstanceOf[IgnitePackDescription].pack.id))
    }
    withTx {
      _ =>
        packs.clearAll(deletes)
        deleteFromCache[String, Array[Byte]](packChunks, { (k, _) =>
          val id = k.split('_').head.toLong
          deletes.contains(id)
        })
        deleteFromCache[String, BinaryObject](packFiles, (_, v) => deletes.contains(v.field("id")))
        saves.foreach(p => {
          packs.replace(p.pack.id, Pack(p.pack.id, p.getPackSource.name(), true, p.getEstimatedPackSize))
        })
    }
  }

  private def deleteFromCache[K, V](cache: IgniteCache[K, V], pred: (K, V) => Boolean): Unit = {
    val cursor = cache.query(new ScanQuery[K, V](new IgniteBiPredicate[K, V] {
      override def apply(k: K, v: V): Boolean = pred(k, v)
    }))
    cursor.forEach(new Consumer[Cache.Entry[K, V]] {
      override def accept(e: Cache.Entry[K, V]): Unit = cache.remove(e.getKey)
    })
    cursor.close()
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

  override def loadChunk(id: String, ext: String, chunk: Int): ByteBuf = {
      val key = s"${id}_${ext}_$chunk"
      val bytes = packChunks.get(key)
      Unpooled.wrappedBuffer(bytes)
  }
}


