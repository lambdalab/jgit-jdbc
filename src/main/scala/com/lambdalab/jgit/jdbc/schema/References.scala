package com.lambdalab.jgit.jdbc.schema

import org.eclipse.jgit.lib.ObjectIdRef.PeeledNonTag
import org.eclipse.jgit.lib.{ObjectId, ObjectIdRef, Ref, SymbolicRef}
import scalikejdbc._

case class Reference(name: String, objectId: String, symbolic: Boolean, target: String)

class References(override val tableName: String, db: NamedDB) extends SQLSyntaxSupport[Reference] {

  override def connectionPoolName: Any = db.name

  def apply(r: ResultName[Reference])(rs: WrappedResultSet): Reference =
    Reference(name = rs.string(r.name),
      objectId = rs.string(r.objectId),
      symbolic = rs.boolean(r.symbolic),
      target = rs.string(r.target)
    )

  def apply(r: SyntaxProvider[Reference])(rs: WrappedResultSet): Reference = apply(r.resultName)(rs)

  def toRef(r: Reference)(implicit session: DBSession): Ref = if (r.symbolic) {
    new SymbolicRef(r.name, targetRef(r))
  } else {
    new ObjectIdRef.PeeledNonTag(Ref.Storage.PACKED, r.name, ObjectId.fromString(r.objectId))
  }

  private def targetRef(r: Reference)(implicit session: DBSession): Ref = {
    getByName(r.target).map(toRef).orNull
  }

  def getByName(name: String)(implicit session: DBSession) = {
    val r = this.syntax("r")
    withSQL {
      select.from(this as r).where.eq(r.name, name)
    }.map(this (r)).single().apply()
  }

  def insertRef(newRef: Ref)(implicit session: DBSession) = {
    val name = newRef.getName
    val objectId = Option(newRef.getObjectId).map(_.name())
    val symbolic = newRef.isSymbolic
    val target = Option(newRef.getTarget).map(_.getName)
    SQL(s"""
          insert into `$tableName` (name,object_id,symbolic,target)
           values({name}, {objectId}, {symbolic}, {target})
          on duplicate key update
            object_id = {objectId},
            symbolic = {symbolic},
            target = {target}
        """).bindByName(
      'name -> name,
      'objectId -> objectId,
      'symbolic -> symbolic,
      'target -> target
    ).update().apply()
  }


  private def toBinds(ref: Ref) = {
    val (newTarget, newObjId) = if (ref.isSymbolic)
      Some(ref.getTarget.getName) -> None else None -> Some(ref.getObjectId.getName)
    Seq(this.column.name -> ref.getName,
      this.column.symbolic -> ref.isSymbolic,
      this.column.target -> newTarget.orNull,
      this.column.objectId -> newObjId.orNull)
  }

  private def equalsToRef(ref: Ref) = {
    val r = this.syntax("r")
    val (target, objId) = if (ref.isSymbolic) {
      Some(ref.getTarget.getName) -> None
    } else {
      None -> Option(ref.getObjectId).map(_.getName)
    }
    sqls.eq(r.name, ref.getName)
        .and
        .eq(r.symbolic, ref.isSymbolic)
        .and(sqls.toAndConditionOpt(
          target.map(t => sqls.eq(r.target, t)),
          objId.map(id => sqls.eq(r.objectId, id))
        ))
  }

  def deleteRef(ref: Ref)(implicit session: DBSession) = {
    val r = this.syntax("r")
    withSQL {
      delete.from(this as r)
          .where(equalsToRef(ref))
    }.execute().apply()
  }

  def updateRef(oldRef: Ref, newRef: Ref)(implicit session: DBSession) = {
    val r = this.syntax("r")
    withSQL {
      update(this as r)
          .set(toBinds(newRef): _*)
          .where(equalsToRef(oldRef))
    }.update().apply()
  }

  def all(implicit session: DBSession) = {
    val r = this.syntax("r")
    withSQL {
      select.from(this as r).orderBy(r.name)
    }.map(this (r)).list().apply()
  }
}
