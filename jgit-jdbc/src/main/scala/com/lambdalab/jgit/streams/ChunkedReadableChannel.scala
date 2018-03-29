package com.lambdalab.jgit.streams

import java.nio.ByteBuffer

import io.netty.buffer.ByteBuf
import org.eclipse.jgit.internal.storage.dfs.ReadableChannel

abstract class ChunkedReadableChannel(chunkSize: Int, localDiskCache: LocalDiskCache) extends ReadableChannel {

  override def setReadAheadBytes(bufferSize: Int): Unit = getChunkByPos

  override def blockSize(): Int = chunkSize

  private var pos: Long = 0

  override def position(): Long = pos

  override def position(newPosition: Long): Unit = {
    pos = newPosition
  }

  private var currentChunk = -1
  private var currentBuff: ByteBuf = _

  def getChunkByPos: ByteBuf = {
    val chunk = (pos / chunkSize).toInt
    if (chunk != currentChunk) {
      currentBuff = localDiskCache.get(chunk)
      currentChunk = chunk
    }
    currentBuff
  }

  override def read(dst: ByteBuffer): Int = {
    if (pos >= size()) {
      return -1
    }
    var read = 0
    while (dst.remaining() > 0) {
      val chunk = getChunkByPos // current or next chunk
      val n = Math.min(dst.remaining(), chunk.readableBytes())
      if (n > 0) {
        val oldlimit = dst.limit()
        dst.limit(dst.position() + n)
        chunk.readBytes(dst)
        read += n
        pos += n
        dst.limit(oldlimit)
      } else {
        return if (read == 0) -1 // reach end before read anything
        else read
      }
    }
    read
  }

  private var open = true

  override def isOpen: Boolean = open

  override def close(): Unit = {
    open = false
    currentChunk = -1
    currentBuff = null
  }
}

object EmptyReadableChannel extends ReadableChannel {
  override def setReadAheadBytes(bufferSize: Int): Unit = {}

  override def blockSize(): Int = 0

  override def size(): Long = 0

  override def position(): Long = 0

  override def position(newPosition: Long): Unit = {}

  override def read(dst: ByteBuffer): Int = 0

  override def isOpen: Boolean = false

  override def close(): Unit = {}
}