package com.lambdalab.jgit.jdbc

import com.lambdalab.jgit.jdbc.schema.{JdbcSchemaDelegate, JdbcSchemaSupport, References}
import org.eclipse.jgit.internal.storage.dfs.DfsRefDatabase
import org.eclipse.jgit.internal.storage.dfs.DfsRefDatabase.RefCache
import org.eclipse.jgit.lib.Ref.Storage
import org.eclipse.jgit.lib._
import org.eclipse.jgit.util.RefList
import scalikejdbc._

class JdbcDfsRefDatabase(repo: JdbcDfsRepository with JdbcSchemaSupport) extends DfsRefDatabase(repo) {
   def db: NamedDB = repo.db
  val refs = new References  with JdbcSchemaDelegate {
    override def delegate = repo
    override val repoName = repo.getDescription.getRepositoryName
  }
  override def compareAndPut(oldRef: Ref, newRef: Ref): Boolean = db localTx {
    implicit s =>
      if (oldRef == null) {
        return refs.insertRef(newRef) > 0
      } else if(oldRef.getStorage == Storage.NEW){
        return refs.insertRef(newRef) > 0
      } else {
        return refs.updateRef(oldRef, newRef) > 0
      }
      return false
  }

  override def scanAllRefs(): DfsRefDatabase.RefCache = db readOnly {
    implicit s =>
      val ids = new RefList.Builder[Ref]
      val sym = new RefList.Builder[Ref]
      val refsMap = refs.all.map(r => r.name -> r).toMap
      refsMap.values.foreach {
        r =>
          if (r.symbolic) {
            val target = refsMap.get(r.target)
                .map(r =>  new ObjectIdRef.PeeledNonTag(Ref.Storage.PACKED,r.name,ObjectId.fromString(r.objectId)))
                    .getOrElse(new ObjectIdRef.Unpeeled(Ref.Storage.NEW, r.target, null))
            val ref = new SymbolicRef(r.name, target)
            sym.add(ref)
            ids.add(ref)
          } else {
            ids.add(new ObjectIdRef.PeeledNonTag(Ref.Storage.PACKED,r.name,ObjectId.fromString(r.objectId)))
          }
      }
      ids.sort()
      sym.sort()
      return new RefCache(ids.toRefList, sym.toRefList)
  }

  override def compareAndRemove(oldRef: Ref): Boolean = db localTx {
    implicit s =>
      refs.deleteRef(oldRef)
  }

  def clear() = db localTx {
    implicit s =>
      refs.clearTable()
  }
}



