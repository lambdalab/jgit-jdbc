package com.lambdalab.jgit.jdbc.steams

import java.nio.ByteBuffer
import java.sql.Blob

import org.eclipse.jgit.internal.storage.dfs.DfsOutputStream

abstract class BlobDfsOutputStream(blob: Blob) extends DfsOutputStream {

  private var pos = 1

  override def read(position: Long, buf: ByteBuffer): Int = {
    val n = Math.min(buf.remaining(), blob.length() - position).toInt
    if (n <= 0) return -1
    val bytes = blob.getBytes(position, n)
    buf.put(bytes)
    n
  }

  override def write(buf: Array[Byte], off: Int, len: Int): Unit = {
    pos += blob.setBytes(pos, buf, off, len)
  }

  def commit(): Unit

  override def close(): Unit = {
    flush()
    commit()
  }
}
