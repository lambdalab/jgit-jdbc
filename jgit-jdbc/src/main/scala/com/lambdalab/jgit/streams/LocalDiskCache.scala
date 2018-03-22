package com.lambdalab.jgit.streams

import java.io.{File, RandomAccessFile}

import com.google.common.cache.AbstractLoadingCache
import com.lambdalab.jgit.streams.LocalDiskCache.{Loader, Builder}
import io.netty.buffer.{ByteBuf, Unpooled}
import org.apache.commons.io.FileUtils

object LocalDiskCache {
  type Loader = (Int) => ByteBuf

  case class Builder(file: File, chunkSize: Int, deleteOnClose: Boolean = true) {
    def build(loader: Loader): LocalDiskCache = {
      new LocalDiskCache(this, loader)
    }
  }

}

class LocalDiskCache(builder: Builder, loader: Loader) extends AbstractLoadingCache[Int, ByteBuf] with AutoCloseable {
  val file = new RandomAccessFile(builder.file, "rw")
  val chunkSize = builder.chunkSize

  private val chunkSet = new java.util.BitSet()

  private def writeChunk(chunk: Int, buf: ByteBuf) = {
    withLock {
      val pos = chunk * chunkSize
      file.seek(pos + buf.readerIndex())
      buf.markReaderIndex()
      buf.readBytes(file.getChannel,buf.readableBytes())
      buf.resetReaderIndex()
      chunkSet.set(chunk)
    }
  }

  override def get(chunk: Int): ByteBuf = {
    if (chunkSet.get(chunk)) {
      readChunk(chunk)
    } else {
      val buf = loader(chunk)
      writeChunk(chunk, buf)
      buf
    }
  }

  private def withLock[T](f: => T): T = {
    val lock = file.getChannel.lock()
    try {
      f
    } finally {
      lock.release()
    }
  }

  private def readChunk(chunk: Int): ByteBuf = {
    val pos = chunk * chunkSize
    withLock {
      val buf = Unpooled.buffer(chunkSize)
      file.seek(pos)
      buf.writeBytes(file.getChannel, chunkSize)
      buf
    }
  }

  override def getIfPresent(key: scala.Any): ByteBuf = {
    val chunk = key.asInstanceOf[Int]
    if (chunkSet.get(chunk)) {
      readChunk(chunk)
    } else null
  }

  override def put(chunk: Int, value: ByteBuf): Unit = {
    writeChunk(chunk, value)
  }

  override def close(): Unit = {
    file.close()
    if (builder.deleteOnClose) {
      FileUtils.deleteQuietly(builder.file)
    }
  }
}
