package com.lambdalab.jgit.jdbc.schema

import java.sql.Blob
import java.util.UUID

import com.lambdalab.jgit.jdbc.JdbcDfsPackDescription
import io.netty.buffer.{ByteBuf, Unpooled}
import scalikejdbc._

case class Pack(repo:String , id: String, source: String, committed: Boolean, estimatedPackSize: Int)

trait Packs extends SQLSyntaxSupport[Pack] {
  self: JdbcSchemaSupport =>
  val repoName: String
  override lazy val tableName = self.packTableName

  override def connectionPoolName: Any = self.db.name

  def insertNew(source: String)(implicit dBSession: DBSession) = {
    val p = this.syntax("p")
    val id = UUID.randomUUID().toString
    withSQL {
      insert.into(this).namedValues(
        this.column.repo -> repoName,
        this.column.id -> id,
        this.column.source -> source,
        this.column.committed -> false,
        this.column.estimatedPackSize -> 0
      )
    }.update().apply()
    Pack(repoName,id, source, committed = false, 0)
  }

  def deleteAll(toDelete: Seq[String])(implicit dBSession: DBSession) = {
    withSQL {
      delete.from(this).where.eq(this.column.repo, repoName).and.in(this.column.id, toDelete)
    }.execute().apply()
  }

  def commitAll(updates: Seq[JdbcDfsPackDescription])(implicit dBSession: DBSession) = {
    updates.foreach { pack =>

      withSQL {
        update(this).set(
          this.column.source -> pack.pack.source,
          this.column.committed -> true,
          this.column.estimatedPackSize -> pack.getEstimatedPackSize
        ).where.eq(this.column.repo, repoName).and.eq(this.column.id, pack.id)
      }.execute().apply()
    }
  }

  def apply(r: ResultName[Pack])(rs: WrappedResultSet): Pack =
    Pack(repo = repoName,
      id = rs.string(r.id),
      source = rs.string(r.source),
      committed = rs.boolean(r.committed),
      estimatedPackSize = rs.int(r.estimatedPackSize)
    )

  def apply(r: SyntaxProvider[Pack])(rs: WrappedResultSet): Pack = apply(r.resultName)(rs)

  def all(implicit dBSession: DBSession) = {
    val p = this.syntax("p")
    withSQL {
      select.from(this as p).where.eq(p.repo, repoName).and.eq(p.committed, true)
    }.map(this (p)).list().apply()
  }

  def dataExts(id: String)(implicit dBSession: DBSession) = {
    val table = new PackFileTable()
    val p = table.syntax("p")

    val results = withSQL{
      select.from(table as p).where.eq(p.repo, repoName).and.eq(p.id , id)
    }.map(table(p)).list().apply()
     results
  }

  def getFile(id:String, ext:String)(implicit dBSession: DBSession) = {
    val table = new PackFileTable()
    val p = table.syntax("p")
     withSQL{
      select.from(table as p).where.eq(p.repo, repoName).and.eq(p.id , id).and.eq(p.ext , ext)
    }.map(table(p)).single().apply()
  }

  def getData(id: String, ext: String, chunk: Int)(implicit dBSession: DBSession) = {
    val data = new PackDataTable()
    val p = data.syntax("p")
    withSQL {
      select.from(data as p).where.eq(p.id, id).and.eq(p.ext, ext).and.eq(p.chunk, chunk)
    }.map(data(p)).single().apply()
  }

  def writeData(id: String, ext: String, chunk: Int, blob: Array[Byte])(implicit dBSession: DBSession): Unit = {
    val table = new PackDataTable()
    val sql = self.getUpsertSql(table.tableName, "id,ext,chunk,data", "?,?,?,?", "chunk=?,data = ?")
    val pstmt = dBSession.connection.prepareStatement(sql)
    pstmt.setString(1, id)
    pstmt.setString(2, ext)
    pstmt.setInt(3, chunk)
    pstmt.setBytes(4, blob)
    pstmt.setBytes(5, blob)
    pstmt.executeUpdate()
    pstmt.close()
  }

  def clearTable()(implicit dBSession: DBSession): Unit = withSQL {
    delete.from(this)
  }.update().apply()

  case class PackData(id: String, ext: String,chunk: Int, data: Blob) {
    lazy val buf: ByteBuf = {
      val bytes = data.getBytes(0, data.length().toInt)
      data.free()
      Unpooled.wrappedBuffer(bytes)
    }
  }

  case class PackFile(id: String, ext: String, size: Int)

  class PackFileTable extends SQLSyntaxSupport[PackFile] {
    override lazy val tableName = self.packFileTableName

    override def connectionPoolName: Any = db.name

    def apply(r: ResultName[PackFile])(rs: WrappedResultSet): PackFile = {
      PackFile(id = rs.string(r.id),
        ext = rs.string(r.ext),
        size = rs.int(r.size)
      )
    }

    def apply(r: SyntaxProvider[PackFile])(rs: WrappedResultSet): PackFile = apply(r.resultName)(rs)
  }

  class PackDataTable extends SQLSyntaxSupport[PackData] {
    override lazy val tableName = self.packDataTableName

    override def connectionPoolName: Any = db.name

    def apply(r: ResultName[PackData])(rs: WrappedResultSet): PackData = {
      PackData(id = rs.string(r.id),
        ext = rs.string(r.ext),
        chunk = rs.int(r.chunk),
        data = self.createBlobFromRs(rs.underlying, r.data)
      )
    }

    def apply(r: SyntaxProvider[PackData])(rs: WrappedResultSet): PackData = apply(r.resultName)(rs)

  }

}


