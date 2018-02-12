package com.lambdalab.jgit.cassandra

import org.eclipse.jgit.internal.storage.dfs.DfsRefDatabase.RefCache
import org.eclipse.jgit.internal.storage.dfs.{DfsRefDatabase, DfsRepository}
import org.eclipse.jgit.lib
import org.eclipse.jgit.lib.Ref.Storage
import org.eclipse.jgit.lib.{ObjectId, ObjectIdRef, Ref => JRef, SymbolicRef}
import org.eclipse.jgit.util.RefList

class CassandraRefDB(repo: CassandraDfsRepo) extends DfsRefDatabase(repo) {

  val refs = new CassandraRefs with CassandraContext {
    override val settings: CassandraSettings = repo.cassandraSettings
  }
  val repoName = repo.getDescription.getRepositoryName

  implicit def toRef(r: lib.Ref) = {
    if (r.isSymbolic) {
      Ref(r.getName, symbolic = true, target = r.getTarget.getName, null)
    } else {
      Ref(r.getName, symbolic = false, null, objectId = r.getObjectId.name())
    }
  }

  override def compareAndPut(oldRef: lib.Ref, newRef: lib.Ref): Boolean = {
    if (oldRef == null) {
      refs.newRef(repoName, newRef)
    } else {
      if (oldRef.getStorage == Storage.NEW) {
        refs.newRef(repoName, newRef)
      } else {
        refs.updateRef(repoName, oldRef, newRef)
      }
    }
  }

  override def scanAllRefs(): DfsRefDatabase.RefCache = {
    val ids = new RefList.Builder[lib.Ref]
    val sym = new RefList.Builder[lib.Ref]
    val all = refs.all(repoName)
    val idMap = all.filterNot(_.symbolic).map {
      r =>
        val ref = new ObjectIdRef.PeeledNonTag(lib.Ref.Storage.PACKED, r.name, ObjectId.fromString(r.objectId))
        ids.add(ref)
        r.name -> ref
    }.toMap

    all.filter(_.symbolic).foreach {
      r =>
        val ref = idMap.get(r.target).map(tag => new SymbolicRef(r.name, tag)).getOrElse {
          new ObjectIdRef.Unpeeled(JRef.Storage.NEW, r.target, null)
        }
        ids.add(ref)
        sym.add(ref)

    }
    ids.sort()
    sym.sort()
    new RefCache(ids.toRefList, sym.toRefList)

  }

  override def compareAndRemove(oldRef: lib.Ref): Boolean = {
    refs.delete(repoName, oldRef)
  }

  def clear(): Unit = {
    refs.clear(repoName)
  }

}
