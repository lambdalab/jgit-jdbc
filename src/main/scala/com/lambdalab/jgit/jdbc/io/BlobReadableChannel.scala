package com.lambdalab.jgit.jdbc.io

import java.nio.ByteBuffer
import java.sql.Blob

import org.eclipse.jgit.internal.storage.dfs.ReadableChannel
import scalikejdbc.DBConnection

class BlobReadableChannel(blob: Blob, conn: DBConnection) extends ReadableChannel {
  val BUFFER_SIZE = 1024 * 4

  override def setReadAheadBytes(bufferSize: Int): Unit = {}

  override def blockSize(): Int = BUFFER_SIZE

  override def size(): Long = blob.length()

  private var pos: Long = 0


  override def position(newPosition: Long): Unit = {
    pos = newPosition
  }

  override def read(dst: ByteBuffer): Int = {
    val n = Math.min(dst.remaining, size - pos).toInt
    if (n == 0) return -1
    dst.put(blob.getBytes(pos + 1 ,n))
    pos += n
    n
  }

  override def isOpen: Boolean = open
  private var open = true
  override def close(): Unit = {
    open = false
    blob.free()
    conn.rollbackIfActive()
    conn.close()
  }

  override def position(): Long = pos
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