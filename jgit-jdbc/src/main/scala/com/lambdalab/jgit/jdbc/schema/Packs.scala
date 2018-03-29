package com.lambdalab.jgit.jdbc.schema

import java.io.InputStream
import java.sql.Blob
import java.util.UUID

import com.lambdalab.jgit.jdbc.JdbcDfsPackDescription
import io.netty.buffer.{ByteBuf, ByteBufInputStream, Unpooled}
import scalikejdbc._

case class Pack(repo: String, id: String, source: String,
                committed: Boolean, estimatedPackSize: Int, files: Seq[PackFile] = Nil)

case class PackData(id: String, ext: String, chunk: Int, data: InputStream)

case class PackFile(id: String, ext: String, size: Int)

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
    Pack(repoName, id, source, committed = false, 0)
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

  def all(implicit dBSession: DBSession): Seq[Pack] = {
    val p = this.syntax("p")
    val packTable = this
    val packFileTable = new PackFileTable()
    val f = packFileTable.syntax("f")
    val sql: SQL[Pack,NoExtractor] = withSQL {
      select.from(this as p).leftJoin(packFileTable as f).on(p.id, f.id)
          .where.eq(p.repo, repoName).and.eq(p.committed, true)
    }
    sql.one(packTable(p))
            .toMany(packFileTable.opt(f))
            .map((pack, files) => pack.copy(files = files))
            .list.apply()
  }

  def getFiles(id: String)(implicit dBSession: DBSession) = {
    val table = new PackFileTable()
    val p = table.syntax("p")

    val results = withSQL {
      select.from(table as p).where.eq(p.id, id)
    }.map(table(p)).list().apply()
    results
  }

  def getFile(id: String, ext: String)(implicit dBSession: DBSession) = {
    val table = new PackFileTable()
    val p = table.syntax("p")
    withSQL {
      select.from(table as p).where.eq(p.id, id).and.eq(p.ext, ext)
    }.map(table(p)).single().apply()
  }

  def getData(id: String, ext: String, chunk: Int)(implicit dBSession: DBSession) = {
    val table = new PackDataTable()
    val p = table.syntax("p")
    withSQL {
      select.from(table as p).where.eq(p.id, id).and.eq(p.ext, ext).and.eq(p.chunk, chunk)
    }.map(table(p)).single().apply()
  }

  def writeData(id: String, ext: String, chunk: Int, data: ByteBuf)(implicit dBSession: DBSession): Unit = {
    val table = new PackDataTable()
    val d = table.syntax("d")
    val exists = withSQL {
      select(d.id).from(table as d).where.eq(d.id, id).and.eq(d.ext, ext).and.eq(d.chunk, chunk)
    }.map(_.string(1)).single().apply().isDefined
    val is: InputStream = new ByteBufInputStream(data)

    if (exists) {
      if (data.readableBytes() > 0) {
        val sql =
          s"""
            update ${table.tableName} set data = $concatExpress
             where id = ? and ext = ? and chunk = ?
          """
        val pstmt = dBSession.connection.prepareStatement(sql)
        pstmt.setBinaryStream(1, is)
        pstmt.setString(2, id)
        pstmt.setString(3, ext)
        pstmt.setInt(4, chunk)
        pstmt.executeUpdate()
        pstmt.close()
      }
    } else {
      withSQL {
        insert.into(table).namedValues(
          table.column.id -> id,
          table.column.ext -> ext,
          table.column.chunk -> chunk,
          table.column.data -> is
        )
      }.executeUpdate().apply()
    }

    /*val sql = self.getUpsertSql(table.tableName, "id,ext,chunk,data", "?,?,?,?", "chunk=?,data = concat(data,?)")
    val pstmt = dBSession.connection.prepareStatement(sql)
    val is = new ByteBufInputStream(data)
    pstmt.setString(1, id)
    pstmt.setString(2, ext)
    pstmt.setInt(3, chunk)
    pstmt.setBinaryStream(4, is)
    pstmt.setInt(5, chunk)
    pstmt.setBinaryStream(6, is)
    pstmt.executeUpdate()
    pstmt.close()
    is.close()*/
  }

  def initFile(id: String, ext: String)(implicit dBSession: DBSession): Unit = {
    val table = new PackFileTable()
    withSQL {
      insert.into(table).namedValues(
        table.column.id -> id,
        table.column.ext -> ext
      )
    }.executeUpdate().apply()
  }

  def updateFileSize(id: String, ext: String, size: Long)(implicit dBSession: DBSession): Unit = {
    val table = new PackFileTable()
    withSQL {
      update(table).set(table.column.size -> size).where
          .eq(table.column.id, id).and.eq(table.column.ext, ext).and.le(table.column.size, size)
    }.executeUpdate().apply()
  }

  def clearTable()(implicit dBSession: DBSession): Unit = withSQL {
    delete.from(this).where.eq(this.column.repo, repoName)
  }.update().apply()

  class PackFileTable extends SQLSyntaxSupport[PackFile] {
    override lazy val tableName = self.packFileTableName

    override def connectionPoolName: Any = db.name

    def apply(r: ResultName[PackFile])(rs: WrappedResultSet): PackFile = {
      PackFile(
        id = rs.string(r.id),
        ext = rs.string(r.ext),
        size = rs.int(r.size)
      )
    }

    def apply(r: SyntaxProvider[PackFile])(rs: WrappedResultSet): PackFile = apply(r.resultName)(rs)
    def opt(m: SyntaxProvider[PackFile])(rs: WrappedResultSet): Option[PackFile] =
      rs.stringOpt(m.resultName.id).map(_ => this(m)(rs))
  }

  class PackDataTable extends SQLSyntaxSupport[PackData] {
    override lazy val tableName = self.packDataTableName

    override def connectionPoolName: Any = db.name

    def apply(r: ResultName[PackData])(rs: WrappedResultSet): PackData = {
      PackData(id = rs.string(r.id),
        ext = rs.string(r.ext),
        chunk = rs.int(r.chunk),
        data = rs.binaryStream(r.data)
      )
    }

    def apply(r: SyntaxProvider[PackData])(rs: WrappedResultSet): PackData = apply(r.resultName)(rs)

  }

}


