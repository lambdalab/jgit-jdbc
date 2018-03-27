package com.lambdalab.jgit.streams

import java.io.File
import java.util.concurrent.ConcurrentHashMap
import java.util.function

import io.netty.buffer.ByteBuf

import scala.collection.JavaConverters._

object LocalFileCacheSupport {
  // make this shared within jvm
  val fileCaches: ConcurrentHashMap[(String, String), LocalDiskCache] = new ConcurrentHashMap[(String, String), LocalDiskCache]()
}

trait LocalFileCacheSupport {

  val chunkSize : Int
  private val tmpdir = System.getProperty("java.io.tmpdir")

  def getFileCache(id: String, ext: String): LocalDiskCache = {
    val file = new File(tmpdir, s"$id.$ext")
    LocalFileCacheSupport.fileCaches.computeIfAbsent(id -> ext, new function.Function[(String,String), LocalDiskCache] {
      override def apply(t: (String, String)): LocalDiskCache =
        LocalDiskCache.Builder(file, chunkSize).build((chunk: Int) => loadChunk(id,ext,chunk))
    })
  }

  def loadChunk(id: String,ext: String, chunk:Int) : ByteBuf

  def closeCaches(): Unit = {
    LocalFileCacheSupport.fileCaches.values().asScala.foreach(_.close())
    LocalFileCacheSupport.fileCaches.clear()
  }

}
