package com.lambdalab.jgit.streams

import java.io.File
import java.util.concurrent.ConcurrentHashMap
import java.util.function

import io.netty.buffer.ByteBuf

import scala.collection.JavaConverters._

trait LocalFileCacheSupport {

  val chunkSize : Int
  val fileCaches: ConcurrentHashMap[(String, String), LocalDiskCache] = new ConcurrentHashMap[(String, String), LocalDiskCache]()
  private val tmpdir = System.getProperty("java.io.tmpdir")

  def getFileCache(id: String, ext: String): LocalDiskCache = {
    val file = new File(tmpdir, s"$id.$ext")
    fileCaches.computeIfAbsent(id -> ext, new function.Function[(String,String), LocalDiskCache] {
      override def apply(t: (String, String)): LocalDiskCache =
        LocalDiskCache.Builder(file, chunkSize).build((chunk: Int) => loadChunk(id,ext,chunk))
    })
  }

  def loadChunk(id: String,ext: String, chunk:Int) : ByteBuf

  def closeCaches(): Unit = {
    fileCaches.values().asScala.foreach(_.close())
    fileCaches.clear()
  }

}
