package com.lambdalab.jgit.streams

import java.io.{File, RandomAccessFile}
import java.nio.channels.FileChannel.MapMode
import java.util.concurrent.atomic.AtomicInteger

import com.google.common.cache.AbstractLoadingCache
import com.lambdalab.jgit.streams.LocalDiskCache.{Builder, Loader}
import io.netty.buffer.{ByteBuf, Unpooled}
import org.apache.commons.io.FileUtils
import org.slf4j.LoggerFactory

object LocalDiskCache {
  type Loader = (Int) => ByteBuf
  val logger = LoggerFactory.getLogger(classOf[LocalDiskCache])

  case class Builder(file: File, chunkSize: Int, deleteOnClose: Boolean = false) {
    def build(loader: Loader): LocalDiskCache = {
      new LocalDiskCache(this, loader)
    }
  }

  val totalCount = new AtomicInteger(0)
  val hitCount = new AtomicInteger(0)
  val dbCount = new AtomicInteger(0)

  def printAndResetCacheRate(): Unit = {
    val db = dbCount.getAndSet(0)
    val hit = hitCount.getAndSet(0)
    val total = totalCount.getAndSet(0)
    println(s"total cache hit rate  hit:$hit db:$db total:$total ${100.0 * hit / total}%")
  }
}

class LocalDiskCache(builder: Builder, loader: Loader) extends AbstractLoadingCache[Int, ByteBuf] with AutoCloseable {
  val chunkSize = builder.chunkSize
  private val chunksFile = new File(builder.file.getParentFile, builder.file.getName + ".chunks")

  import java.util.concurrent.locks.ReentrantLock
  import LocalDiskCache._

  val lock = new ReentrantLock
  private val chunkSet = {
    val chunkSetFile = new RandomAccessFile(chunksFile, "rw")
    try {
      val bytes = new Array[Byte](chunkSetFile.length().toInt)
      chunkSetFile.readFully(bytes)
      java.util.BitSet.valueOf(bytes)
    } finally {
      chunkSetFile.close()
    }
  }

  def writeChunkSet() {
    val chunkSetFile = new RandomAccessFile(chunksFile, "rw")
    try {
      chunkSetFile.seek(0)
      chunkSetFile.write(chunkSet.toByteArray)
    } finally{
      chunkSetFile.close()
    }
  }

  private def writeChunk(chunk: Int, buf: ByteBuf) = {

    withLock {  file =>
      val pos = chunk * chunkSize
      file.getChannel().position(pos + buf.readerIndex())
      buf.markReaderIndex()
      buf.readBytes(file.getChannel, buf.readableBytes())
      buf.resetReaderIndex()
      chunkSet.set(chunk)
      writeChunkSet()
    }
  }

  override def get(chunk: Int): ByteBuf = {
    totalCount.getAndIncrement()
    if (chunkSet.get(chunk)) {
      hitCount.getAndIncrement()
      readChunk(chunk)
    } else {
      val buf = loader(chunk)
      dbCount.getAndIncrement()
      try {
        writeChunk(chunk, buf)
      } catch {
        case ex: Throwable => logger.error("", ex)
      }
      buf
    }
  }

  private def withLock[T](f: RandomAccessFile => T): T = {
    val file = new RandomAccessFile(builder.file, "rw")
    lock.lock() // file lock is shared within jvm , so we need another lock to guard it
    val flock = file.getChannel.lock()
    try {
      f(file)
    } finally {
      flock.release()
      lock.unlock()
      file.close()
    }
  }

  private def readChunk(chunk: Int): ByteBuf = {
    val pos = chunk * chunkSize
    withLock { file =>
      val size = Math.min(file.length() - pos, chunkSize)
      val bb = file.getChannel.map(MapMode.READ_ONLY, pos, size)
      Unpooled.wrappedBuffer(bb)
    }
  }

  override def getIfPresent(key: scala.Any): ByteBuf = {
    val chunk = key.asInstanceOf[Int]
    if (chunkSet.get(chunk)) {
      readChunk(chunk)
    } else null
  }

  override def put(chunk: Int, value: ByteBuf): Unit = {
    try {
      writeChunk(chunk, value)
    } catch {
      case ex: Throwable => logger.error("", ex)
    }
  }

  override def close(): Unit = {
    if (builder.deleteOnClose) {
      FileUtils.deleteQuietly(builder.file)
      FileUtils.deleteQuietly(chunksFile)
    }
  }
}
