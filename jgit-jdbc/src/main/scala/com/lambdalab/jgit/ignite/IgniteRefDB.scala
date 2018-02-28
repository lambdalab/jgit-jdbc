package com.lambdalab.jgit.ignite

import org.apache.ignite.IgniteCache
import org.apache.ignite.binary.BinaryObject
import org.apache.ignite.cache.query.ScanQuery
import org.eclipse.jgit.internal.storage.dfs.DfsRefDatabase
import org.eclipse.jgit.internal.storage.dfs.DfsRefDatabase.RefCache
import org.eclipse.jgit.lib
import org.eclipse.jgit.lib.Ref.Storage
import org.eclipse.jgit.lib.{ObjectId, ObjectIdRef, Ref, SymbolicRef}
import org.eclipse.jgit.util.RefList

import scala.collection.JavaConversions._

case class IgniteRef(name: String,
                     symbolic: Boolean,
                     target: String,
                     objectId: String) {

}

class IgniteRefDB(repo: IgniteRepo) extends DfsRefDatabase(repo)   {

  private val refsCacheName = s"${repo.getDescription.getRepositoryName}_refs"
  val refs: IgniteCache[String, BinaryObject] = repo.ignite.getOrCreateCache(refsCacheName).withKeepBinary()

  def clear(): Unit = refs.clear()

  def delete(): Unit = refs.destroy()

  val FIELD_NAME = "name"
  val FIELD_SYMBOLIC = "symbolic"
  val FIELD_OBJECT_ID = "objectId"
  val FILED_TARGET = "target"

  implicit def ref2bin(ref: Ref): BinaryObject = {
     val builder = repo.ignite.binary.builder("com.lambdalab.jgit.ignite.IgniteRef")
    if (ref.isSymbolic) {
      builder.setField(FIELD_NAME, ref.getName)
          .setField(FIELD_SYMBOLIC, true)
          .setField(FILED_TARGET, ref.getTarget.getName)
          .build()
    } else {
      builder.setField(FIELD_NAME, ref.getName)
          .setField(FIELD_SYMBOLIC, false)
          .setField(FIELD_OBJECT_ID, ref.getObjectId.getName)
          .build()
    }
  }

  override def compareAndPut(oldRef: Ref, newRef: Ref): Boolean = {
    if (oldRef == null) {
      refs.putIfAbsent(newRef.getName, newRef)
    } else {
      if (oldRef.getStorage == Storage.NEW) {
        refs.putIfAbsent(newRef.getName, newRef)
      } else {
        val o = ref2bin(oldRef)
        repo.withTx {
          _ =>
          val old = refs.get(newRef.getName)
            if(old!=null && old.equals(o)) {
              refs.replace(newRef.getName, ref2bin(newRef))
            } else
            false
        }
      }
    }
  }

  override def scanAllRefs(): DfsRefDatabase.RefCache = {
    val ids = new RefList.Builder[lib.Ref]
    val sym = new RefList.Builder[lib.Ref]

    val all = refs.query(new ScanQuery[String, BinaryObject]()).getAll.toSeq.map(_.getValue)
    val idMap = all.filterNot(_.field[Boolean](FIELD_SYMBOLIC)).map {
      r =>
        val name = r.field[String](FIELD_NAME)
        val objectId = r.field[String](FIELD_OBJECT_ID)
        val ref = new ObjectIdRef.PeeledNonTag(lib.Ref.Storage.PACKED, name, ObjectId.fromString(objectId))
        ids.add(ref)
        name -> ref
    }.toMap

    all.filter(_.field[Boolean](FIELD_SYMBOLIC)).foreach {
      r =>
        val name = r.field[String](FIELD_NAME)
        val target: String = r.field(FILED_TARGET)
        val ref = idMap.get(target).map(tag => new SymbolicRef(name, tag)).getOrElse {
          new ObjectIdRef.Unpeeled(Ref.Storage.NEW, target, null)
        }
        ids.add(ref)
        sym.add(ref)

    }
    ids.sort()
    sym.sort()
    new RefCache(ids.toRefList, sym.toRefList)
  }

  override def compareAndRemove(ref: Ref): Boolean = {
    refs.remove(ref.getName, ref)
  }
}
