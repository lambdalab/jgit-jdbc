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
      refs.all.foreach {
        r =>
          if (r.symbolic) {
            sym.add(refs.toRef(r))
          } else {
            ids.add(refs.toRef(r))
          }
      }
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



