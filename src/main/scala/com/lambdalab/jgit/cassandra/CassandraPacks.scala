package com.lambdalab.jgit.cassandra

import java.nio.ByteBuffer
import java.util.UUID

import com.datastax.driver.core.{BatchStatement, Session}
import com.lambdalab.jgit.streams.{ChunkedDfsOutputStream, ChunkedReadableChannel}
import io.netty.buffer.{ByteBuf, Unpooled}
import org.eclipse.jgit.internal.storage.dfs.{DfsOutputStream, ReadableChannel}

import scala.collection.JavaConverters._

case class Pack(id: UUID, source: String, committed: Boolean, estimatedPackSize: Int)

case class PackData(id: UUID, ext: String, blobs: List[ByteBuffer])
object CassandraPacks {
  val schema =
    """CREATE TABLE packs (
                        repo text,
                        id uuid,
                        source text,
                        committed boolean,
                        estimated_pack_size int,
                        PRIMARY KEY (repo, id)
                      );"""
  val dataSchema =
    """CREATE TABLE packs_data (
                        repo text,
                        id uuid,
                        ext text,
                        chunk int,
                        end bigint,
                        data blob,
                        PRIMARY KEY (repo, id , ext, chunk)
                      );"""

  def createSchema(settings: CassandraSettings): Unit = {
    val ks = settings.cluster.getMetadata.getKeyspace(settings.keyspace)
    if (ks.getTable("packs") == null) {
      settings.session.execute(schema)
    }
    if (ks.getTable("packs_data") == null) {
      settings.session.execute(dataSchema)
    }
  }
}
trait CassandraPacks {
  self: CassandraContext =>

  val chunkSize = 1024 * 100



  def insertNew(repo: String, source: String): Pack = {
    val cql = """insert into packs(repo,id,source,committed) values(?,?,?,false)"""
    val id = UUID.randomUUID()
    if (execute(cql)(_.bind(repo, id, source)).wasApplied()) {
      Pack(id, source, committed = false, 0)
    } else {
      null
    }
  }

  def packDataExt(repo: String, id: UUID) = {
    val cql =
      """select ext, max(end) as size  from packs_data where repo = ?
        and id = ? group by ext"""
    execute(cql)(_.bind(repo, id)).all().asScala.map {
      r =>
        r.getString("ext") -> r.getLong("size")
    }
  }

  def allCommitted(repo: String): Seq[Pack] = {
    val cql ="""select id,source,estimated_pack_size from packs where repo = ? and committed = true allow filtering"""
    execute(cql)(_.bind(repo)).all().asScala.map(
      r => Pack(r.getUUID("id"),
        source = r.getString("source"),
        committed = true,
        estimatedPackSize = r.getInt("estimated_pack_size")))
  }

  def batchDelete(repo: String, packs: Iterable[CassandraDfsPackDescription]) = {
    val batch = new BatchStatement()
    val deletes = packs.map(_.id)
    batch.addAll(deleteStatements(repo, deletes).asJava)
    if (session.execute(batch).wasApplied()) {
      if (deletes.nonEmpty)
        deleteData(repo, deletes)
      else
        false
    } else {
      false
    }
  }

  private def deleteStatements(repo: String, deletes: Iterable[UUID]) = {
    deletes.map {
      id =>
        val stmt = statmentCache.apply("DELETE FROM packs where repo = ? and id = ? if exists")(session.prepare)
        stmt.bind(repo, id)
    }
  }

  private def deleteData(repo: String, deletes: Iterable[UUID]) = {
    val deleteIds = deletes.map(_.toString).mkString(",")
    val cql =
      s"""select chunk, ext, id from packs_data where repo = ? and id in
             ($deleteIds)"""

    val rs = session.execute(session.prepare(cql).bind(repo))
    val stmt = statmentCache
        .apply("DELETE FROM packs_data where repo =? and id = ? and ext=? and chunk =?")(session.prepare)
    val batch = new BatchStatement()
    rs.all().asScala.foreach {
      row =>
        batch.add(
          stmt.bind(repo, row.getUUID("id"), row.getString("ext"), Integer.valueOf(row.getInt("chunk")))
        )
    }
    session.execute(batch).wasApplied()
  }

  def commitAll(repo: String, commits: Iterable[CassandraDfsPackDescription],
                deletesPacks: Iterable[CassandraDfsPackDescription]) = {
    val batch = new BatchStatement()
    val deletes = deletesPacks.map(_.id)
    batch.addAll(deleteStatements(repo, deletes).asJava)
    commits.foreach {
      p =>
        val stmt = statmentCache.apply(
          "UPDATE packs set committed = true, estimated_pack_size = ?" +
              " where repo = ? and id = ? if exists")(session.prepare)
        batch.add(stmt.bind(Integer.valueOf(p.getEstimatedPackSize.toInt), repo, p.id))
    }
    val rs = session.execute(batch)
    if (rs.wasApplied()) {
      if (deletes.nonEmpty) {
        deleteData(repo, deletes)
      } else
        true
    } else {
      false
    }
  }

  def readPack(repo: String, id: UUID, ext: String): ReadableChannel = {
    new ChunkedReadableChannel(chunkSize) {
      private lazy val _size = {
        val cql = """select max(end) from packs_data where repo = ? and id = ? and ext = ?"""
        execute(cql)(_.bind(repo, id, ext)).one().getLong(0)
      }

      override def size(): Long = _size

      override def readChunk(chunk: Int): ByteBuf = {
        val cql = """select data from packs_data where repo = ? and id = ? and ext = ? and chunk = ?"""

        val bb = execute(cql)(_.bind(repo, id, ext, Integer.valueOf(chunk))).one().getBytes(0)
        Unpooled.wrappedBuffer(bb)
      }
    }
  }

  def writePack(repo: String, id: UUID, ext: String): DfsOutputStream = {
    new ChunkedDfsOutputStream(chunkSize) {
      def flushBuffer(h: BufHolder): Unit = {
        val BufHolder(buf, chunk) = h
        val cql =
          """update packs_data set data = ? , end = ?
              where repo = ? and id = ? and ext = ? and chunk = ?"""
        execute(cql)(_.bind(buf.nioBuffer(), java.lang.Long.valueOf(h.end), repo, id, ext, Integer.valueOf(chunk)))
      }

      override def readFromDB(chunk: Int): ByteBuf = {
        val bb = execute("""select data from packs_data where repo = ? and id = ? and ext = ? and chunk = ?""") {
          _.bind(repo, id, ext, Integer.valueOf(chunk))
        }.one().getBytes(0)
        Unpooled.wrappedBuffer(bb)
      }
    }
  }

  def clear(repo: String): Unit = {
    execute("delete from packs where repo = ? ")(_.bind(repo))
    execute("delete from packs_data where repo = ?")(_.bind(repo))
  }
}
