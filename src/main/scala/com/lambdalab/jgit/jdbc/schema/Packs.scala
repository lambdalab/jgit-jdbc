package com.lambdalab.jgit.jdbc.schema

import java.sql.Blob

import scalikejdbc._

case class Pack(id: Long, source: String, committed: Boolean)

class Packs(override val tableName: String, db: NamedDB) extends SQLSyntaxSupport[Pack] {

  override def connectionPoolName: Any = db.name

  def insertNew(source: String)(implicit dBSession: DBSession) = {
    val p = this.syntax("p")
    val id = withSQL {
      insert.into(this).namedValues(
        this.column.source -> source,
        this.column.committed -> false
      )
    }.updateAndReturnGeneratedKey().apply()
    Pack(id, source, false)
  }

  def deleteAll(toDelete: Seq[Long])(implicit dBSession: DBSession) = {
    val p = this.syntax("p")
    withSQL {
      delete.from(this as p).where.in(p.id, toDelete)
    }.execute().apply()
  }

  def commitAll(updates: Seq[Pack])(implicit dBSession: DBSession) = {
    val p = this.syntax("p")
    updates.foreach { pack =>
      withSQL {
        update(this as p).set(
          p.source -> pack.source,
          p.committed -> true
        ).where.eq(p.id, pack.id)
      }.execute().apply()
    }
  }

  def apply(r: ResultName[Pack])(rs: WrappedResultSet): Pack =
    Pack(id = rs.int(r.id),
      source = rs.string(r.source),
      committed = rs.boolean(r.committed)
    )

  def apply(r: SyntaxProvider[Pack])(rs: WrappedResultSet): Pack = apply(r.resultName)(rs)

  def all(implicit dBSession: DBSession) = {
    val p = this.syntax("p")
    withSQL {
      select.from(this as p).where.eq(p.committed, true)
    }.map(this (p)).list().apply()
  }

  def getData(id: Long, ext: String)(implicit dBSession: DBSession) = {
    val table = new PackDataTable()
    val p = table.syntax("p")
    withSQL {
      select.from(table as p).where.eq(p.id, id).and.eq(p.ext, ext)
    }.map(table(p)).single().apply()
  }

  def writeData(id: Long, ext: String, blob: Blob)(implicit dBSession: DBSession): Unit = {
    val table = new PackDataTable()
    SQL(
      s"""
      insert into `${table.tableName}`(id,ext,data) values({id}, {ext}, {data})
      on duplicate key update  data = {data}
    """)
        .bindByName(
          'id -> id,
          'ext -> ext,
          'data -> blob)
        .update().apply()
  }

  case class PackData(id: Long, ext: String, data: Blob)

  class PackDataTable extends SQLSyntaxSupport[PackData] {
    override val tableName = s"${Packs.this.tableName}_data"

    override def connectionPoolName: Any = db.name

    def apply(r: ResultName[PackData])(rs: WrappedResultSet): PackData = {
      PackData(id = rs.int(r.id),
        ext = rs.string(r.ext),
        data = rs.blob(r.data)
      )
    }

    def apply(r: SyntaxProvider[PackData])(rs: WrappedResultSet): PackData = apply(r.resultName)(rs)

  }

}


