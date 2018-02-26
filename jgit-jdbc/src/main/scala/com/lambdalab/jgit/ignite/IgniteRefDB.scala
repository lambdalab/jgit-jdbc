package com.lambdalab.jgit.ignite

import org.apache.ignite.IgniteCache
import org.apache.ignite.cache.query.ScanQuery
import org.apache.ignite.cache.query.annotations.QuerySqlField
import org.eclipse.jgit.internal.storage.dfs.DfsRefDatabase
import org.eclipse.jgit.internal.storage.dfs.DfsRefDatabase.RefCache
import org.eclipse.jgit.lib
import org.eclipse.jgit.lib.Ref.Storage
import org.eclipse.jgit.lib.{ObjectId, ObjectIdRef, Ref, SymbolicRef}
import org.eclipse.jgit.util.RefList

import scala.collection.JavaConversions._

case class IgniteRef(@QuerySqlField(index = true) var name: String,
                @QuerySqlField var symbolic: Boolean,
                @QuerySqlField var target: String,
                @QuerySqlField var objectId: String) extends java.io.Serializable {

}

class IgniteRefDB(repo: IgniteRepo) extends DfsRefDatabase(repo) {

  private val refsCacheName = s"${repo.getDescription.getRepositoryName}_refs"
  val refs: IgniteCache[String, IgniteRef] = repo.ignite.getOrCreateCache(refsCacheName)

  def clear(): Unit =  refs.clear()
  def delete() = refs.destroy()

  private def toIgniteRef(ref: Ref): IgniteRef = {
    if (ref.isSymbolic) {
      new IgniteRef(ref.getName, true, ref.getTarget.getName, null)
    } else {
      new IgniteRef(ref.getName, false , null, ref.getObjectId.getName)
    }
  }

  override def compareAndPut(oldRef: Ref, newRef: Ref): Boolean = {
    val n = toIgniteRef(newRef)
    if (oldRef == null) {
      refs.putIfAbsent(n.name, n)
    } else {
      if (oldRef.getStorage == Storage.NEW) {
        refs.putIfAbsent(n.name, n)
      } else {
        val o = toIgniteRef(oldRef)
        refs.replace(n.name, o, n)
      }
    }
  }

  override def scanAllRefs(): DfsRefDatabase.RefCache = {
    val ids = new RefList.Builder[lib.Ref]
    val sym = new RefList.Builder[lib.Ref]

    val all = refs.query(new ScanQuery[String,IgniteRef]()).getAll.toSeq.map(_.getValue)
    val idMap = all.filterNot(_.symbolic).map {
      r =>
        val ref = new ObjectIdRef.PeeledNonTag(lib.Ref.Storage.PACKED, r.name, ObjectId.fromString(r.objectId))
        ids.add(ref)
        r.name -> ref
    }.toMap

    all.filter(_.symbolic).foreach {
      r =>
        val ref = idMap.get(r.target).map(tag => new SymbolicRef(r.name, tag)).getOrElse {
          new ObjectIdRef.Unpeeled(Ref.Storage.NEW, r.target, null)
        }
        ids.add(ref)
        sym.add(ref)

    }
    ids.sort()
    sym.sort()
    new RefCache(ids.toRefList, sym.toRefList)
  }

  override def compareAndRemove(ref: Ref): Boolean = {
    refs.remove(ref.getName, toIgniteRef(ref))
  }
}
