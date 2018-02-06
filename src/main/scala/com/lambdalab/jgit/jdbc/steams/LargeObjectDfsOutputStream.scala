package com.lambdalab.jgit.jdbc.steams

import java.nio.ByteBuffer

import org.eclipse.jgit.internal.storage.dfs.DfsOutputStream
import org.postgresql.largeobject.LargeObject

abstract class LargeObjectDfsOutputStream(blob: LargeObject) extends DfsOutputStream {
  private var pos = 0

  override def read(position: Long, buf: ByteBuffer): Int = {
    val n = Math.min(buf.remaining(), blob.size() - position).toInt
    if (n <= 0) return -1
    blob.seek(position.toInt)
    val bytes = blob.read(n)
    buf.put(bytes)
    n
  }

  override def write(buf: Array[Byte], off: Int, len: Int): Unit = {
    blob.seek(pos)
    blob.write(buf, off, len)
    pos += len
  }

  def commit(): Unit

  override def close(): Unit = {
    blob.close()
    flush()
    commit()
  }
}