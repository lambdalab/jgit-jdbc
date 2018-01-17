package com.lambdalab.jgit.cassandra

import java.nio.ByteBuffer
import java.util.UUID

import com.datastax.driver.core.{BatchStatement, BoundStatement, PreparedStatement}
import org.eclipse.jgit.internal.storage.dfs.{DfsOutputStream, ReadableChannel}

import scala.collection.JavaConverters._

case class Pack(repo: String, id: UUID, source: String, committed: Boolean)

case class PackData(repo: String, id: UUID, ext: String, blobs: List[ByteBuffer])

trait CassandraPacks {
  self: CassandraContext =>

  val chunkSize = 1024 * 100

  val schema =
    """CREATE TABLE packs (
                        repo text,
                        id uuid,
                        source text,
                        committed boolean,
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

  def insertNew(repo: String, source: String): Pack = {
    val cql = """insert into packs(repo,id,source,committed) values(?,?,?,false)"""
    val id = UUID.randomUUID()
    if (execute(cql)(_.bind(repo, id, source)).wasApplied()) {
      Pack(repo, id, source, committed = false)
    } else {
      null
    }
  }

  def allCommitted(repo: String): Seq[Pack] = {
    val cql ="""select id,source from packs where repo = ? and committed =true allow filtering"""
    execute(cql)(_.bind(repo)).all().asScala.map(
      r => Pack(repo,r.getUUID("id"), r.getString("source"), committed = true))
  }

  def batchDelete(repo: String, deletes: Seq[UUID]) = {
    val batch = new BatchStatement()
    batch.addAll(deleteStatements(repo, deletes).asJava)
    session.execute(batch).wasApplied()
  }

  private def deleteStatements(repo: String, deletes: Seq[UUID]) = {
    deletes.flatMap {
      id =>
        val stmt = statmentCache.apply("DELETE FROM packs where repo = ? and id = ?")(session.prepare)
        val stmt2 = statmentCache.apply(
          "DELETE FROM packs_data where repo =? and id = ? and ext=? and chunk =?")(session.prepare)
        val chunks = execute("select ext,chunk from packs_data where repo=? and id =?")(_.bind(repo, id))
            .all().asScala.map(
          row =>
              stmt2.bind(repo,id, row.getString("ext"), Integer.valueOf(row.getInt("chunk")))
        )
        stmt.bind(repo, id) :: chunks.toList
    }
  }

  def commitAll(repo:String, commits: Seq[UUID], deletes: Seq[UUID]) = {
    val batch = new BatchStatement()
    batch.addAll(deleteStatements(repo,deletes).asJava)
    commits.foreach {
      id =>
        val stmt = statmentCache.apply(
          "UPDATE packs set committed = true where repo = ? and id = ? if exists")(session.prepare)
        batch.add(stmt.bind(repo, id))
    }
    session.execute(batch).wasApplied()
  }


  def readPack(repo: String, id: UUID, ext: String): ReadableChannel = {
    new ChunkedReadableChannel(chunkSize) {
      private lazy val _size = {
        val cql = """select max(end) from packs_data where repo = ? and id = ? and ext = ?"""
        execute(cql)(_.bind(repo, id, ext)).one().getLong(0)
      }

      override def size(): Long = _size

      override def readChunk(chunk: Int): ByteBuffer = {
        val cql = """select data from packs_data where repo = ? and id = ? and ext = ? and chunk = ?"""
        execute(cql)(_.bind(repo, id, ext, Integer.valueOf(chunk))).one().getBytes(0)
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

      override def readFromDB(chunk: Int): ByteBuffer = {
        execute("""select data from packs_data where repo = ? and id = ? and ext = ? and chunk = ?""") {
          _.bind(repo, id, ext, Integer.valueOf(chunk))
        }.one().getBytes(0)
      }
    }
  }

  private def execute(cql: String)(bindFunc: PreparedStatement => BoundStatement) = {
    val stmt = statmentCache.apply(cql)(session.prepare)
    val bound = bindFunc(stmt)
    session.execute(bound)
  }

  def clear(): Unit = {
    execute("TRUNCATE packs")(_.bind)
    execute("TRUNCATE packs_data")(_.bind)
  }
}
