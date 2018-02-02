package com.lambdalab.jgit.jdbc.schema

import java.sql.Blob

import com.lambdalab.jgit.jdbc.JdbcDfsPackDescription
import scalikejdbc._

import scala.collection.mutable

case class Pack(id: Long, source: String, committed: Boolean, estimatedPackSize: Int)

trait Packs extends SQLSyntaxSupport[Pack] {
  self: JdbcSchemaSupport =>

  override lazy val tableName = self.packTableName

  override def connectionPoolName: Any = self.db.name

  def insertNew(source: String)(implicit dBSession: DBSession) = {
    val p = this.syntax("p")
    val id = withSQL {
      insert.into(this).namedValues(
        this.column.source -> source,
        this.column.committed -> false,
        this.column.estimatedPackSize -> 0
      )
    }.updateAndReturnGeneratedKey().apply()
    Pack(id, source, false, 0)
  }

  def deleteAll(toDelete: Seq[Long])(implicit dBSession: DBSession) = {
    withSQL {
      delete.from(this).where.in(this.column.id, toDelete)
    }.execute().apply()
  }

  def commitAll(updates: Seq[JdbcDfsPackDescription])(implicit dBSession: DBSession) = {
    updates.foreach { pack =>

      withSQL {
        update(this).set(
          this.column.source -> pack.pack.source,
          this.column.committed -> true,
          this.column.estimatedPackSize -> pack.getEstimatedPackSize
        ).where.eq(this.column.id, pack.id)
      }.execute().apply()
    }
  }

  def apply(r: ResultName[Pack])(rs: WrappedResultSet): Pack =
    Pack(id = rs.int(r.id),
      source = rs.string(r.source),
      committed = rs.boolean(r.committed),
      estimatedPackSize = rs.int(r.estimatedPackSize)
    )

  def apply(r: SyntaxProvider[Pack])(rs: WrappedResultSet): Pack = apply(r.resultName)(rs)

  def all(implicit dBSession: DBSession) = {
    val p = this.syntax("p")
    withSQL {
      select.from(this as p).where.eq(p.committed, true)
    }.map(this (p)).list().apply()
  }

  def dataExts(id: Long)(implicit dBSession: DBSession) = {
    val table = new PackDataTable()
    val sql =
      s"""
          select ext, LENGTH(data) from ${table.tableName} where id = ?
        """
    val pstmt = dBSession.connection.prepareStatement(sql)
    pstmt.setLong(1, id)
    val rs = pstmt.executeQuery()
    val result = mutable.Buffer[(String, Int)]()
    while (rs.next()) {
      val tuple = rs.getString(1) -> rs.getInt(2)
      result.append(tuple)
    }
    result
  }

  def getData(id: Long, ext: String)(implicit dBSession: DBSession) = {
    val table = new PackDataTable()
    val p = table.syntax("p")
    withSQL {
      select.from(table as p).where.eq(p.id, id).and.eq(p.ext, ext)
    }.map(table(p)).single().apply()
  }

  def writeData(id: Long, ext: String, blob: Any)(implicit dBSession: DBSession): Unit = {
    val table = new PackDataTable()
    val sql = self.getUpsertSql(table.tableName, "id,ext,data", "?,?,?", "data = ?")
    val pstmt = dBSession.connection.prepareStatement(sql)
    pstmt.setLong(1, id)
    pstmt.setString(2, ext)
    blob match {
      case l: Long =>
        pstmt.setLong(3, l)
        pstmt.setLong(4, l)
      case b: Blob =>
        pstmt.setBlob(3, b)
        pstmt.setBlob(4, b)
      case _ =>
        pstmt.setObject(3, blob)
        pstmt.setObject(4, blob)
    }
    pstmt.executeUpdate()
    pstmt.close()
  }

  def clearTable()(implicit dBSession: DBSession): Unit = withSQL {
    delete.from(this)
  }.update().apply()

  case class PackData(id: Long, ext: String, data: Blob)

  class PackDataTable extends SQLSyntaxSupport[PackData] {
    override lazy val tableName = self.packDataTableName

    override def connectionPoolName: Any = db.name

    def apply(r: ResultName[PackData])(rs: WrappedResultSet): PackData = {
      PackData(id = rs.int(r.id),
        ext = rs.string(r.ext),
        data = self.createBlobFromRs(rs.underlying, r.data)
      )
    }

    def apply(r: SyntaxProvider[PackData])(rs: WrappedResultSet): PackData = apply(r.resultName)(rs)

  }

}


