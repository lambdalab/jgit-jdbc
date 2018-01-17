package com.lambdalab.jgit.cassandra

import java.nio.ByteBuffer

import io.netty.buffer.Unpooled
import org.eclipse.jgit.internal.storage.dfs.DfsOutputStream

abstract class ChunkedDfsOutputStream(val chunkSize: Int) extends DfsOutputStream {

  import io.netty.buffer.ByteBuf

 
  case class BufHolder(buf: ByteBuf, chunk: Int) {
    def offset =  chunkSize.toLong * chunk
    def end = offset + buf.writerIndex()
  }

  var current = BufHolder(Unpooled.buffer(chunkSize), 0)

  def readFromDB(idx: Int): ByteBuffer

  override def read(position: Long, buf: ByteBuffer): Int = {
    var read = 0
    while(buf.remaining() > 0) {
      val pos = position + read
      if (pos < current.offset) {
        val idx = pos / chunkSize
        val readBuf = Unpooled.wrappedBuffer(readFromDB(idx.toInt))
        read += readFrom(buf, readBuf)
      } else if (pos >= current.offset && pos < current.end) {
        read += readFrom(buf, current.buf)
      } else {
        return read
      }
    }
    read
  }

  private def readFrom(buf: ByteBuffer, from: ByteBuf) = {
    val n = Math.min(from.readableBytes(), buf.remaining())
    val oldLimit = buf.limit()
    buf.limit(buf.position()+ n)
    from.readBytes(buf)
    buf.limit(oldLimit)
    n
  }

  override def write(b: Array[Byte], off: Int, len: Int): Unit = {
    val buf = current.buf
    val writeLen = Math.min(len, buf.writableBytes())
    buf.writeBytes(b, off, writeLen)
    val remain = len - writeLen
    if (remain > 0) {
      flush()
      current = BufHolder(Unpooled.buffer(chunkSize), current.chunk + 1)
      write(b, writeLen, remain)
    }
  }

  override def blockSize(): Int = chunkSize

  def flushBuffer(current: BufHolder): Unit

  override def flush() = {
    flushBuffer(current)
  }

  override def close(): Unit = {
    flush()
    super.close()
  }
}
