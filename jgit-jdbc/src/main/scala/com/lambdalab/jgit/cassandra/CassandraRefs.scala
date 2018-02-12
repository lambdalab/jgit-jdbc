package com.lambdalab.jgit.cassandra

import com.datastax.driver.core.Row

import scala.collection.JavaConverters._

case class Ref(name: String, symbolic: Boolean, target: String, objectId: String)

object CassandraRefs {
  val schema =
    """CREATE TABLE refs (
                        repo text,
                        name text,
                        symbolic boolean,
                        object_id text,
                        target text,
                        PRIMARY KEY (repo, name)
                      );"""

  def createSchema(settings: CassandraSettings): Unit = {
    val ks = settings.cluster.getMetadata.getKeyspace(settings.keyspace)
    if (ks.getTable("refs") == null) {
      settings.session.execute(schema)
    }
  }
}

trait CassandraRefs {
  self: CassandraContext =>

  def clear(name: String): Unit = {
    execute("delete from refs where repo = ? ")(_.bind(name))
  }

  def newRef(repo: String, ref: Ref): Boolean = {
    val Ref(name, symbolic, target, objectId) = ref
    if (symbolic) {
      execute("insert into refs(repo,name,symbolic,target) values(?,?,true,?) ")(
        _.bind(repo, name, target)).wasApplied()
    } else {
      execute("insert into refs(repo,name,symbolic,object_id) values(?,?,false,?) ")(
        _.bind(repo, name, objectId)).wasApplied()
    }
  }

  def updateRef(repo: String, oldRef: Ref, newRef: Ref): Boolean = {
    val Ref(name, symbolic, target, objectId) = oldRef
    if (symbolic) {
      execute("update refs set target = ?,  symbolic = ?, object_id = ?" +
          " where repo =? and name = ? if symbolic = true and target = ?")(
        _.bind(newRef.target, java.lang.Boolean.valueOf(newRef.symbolic), newRef.objectId,
          repo, name, target)).wasApplied()
    } else {
      execute("update refs set target = ?,  symbolic = ?, object_id = ?" +
          " where repo =? and name = ? if symbolic = false and object_id = ?")(
        _.bind(newRef.target, java.lang.Boolean.valueOf(newRef.symbolic), newRef.objectId,
          repo, name, objectId)).wasApplied()

    }
  }

  private def rowToRef(row: Row) = {
    Ref(
      name = row.getString("name"),
      symbolic = row.getBool("symbolic"),
      objectId = row.getString("object_id"),
      target = row.getString("target")
    )
  }

  def all(repo: String): Seq[Ref] = {
    execute("select name,symbolic, object_id, target from refs where repo = ?")(_.bind(repo))
        .all().asScala.map(rowToRef)
  }

  def delete(repo: String, ref: Ref): Boolean = {
    val Ref(name, symbolic, target, objectId) = ref
    if (symbolic) {
      execute("delete from refs where repo =? and name = ? if symbolic =true and target = ?")(
        _.bind(repo, name, target)).wasApplied()
    } else {
      execute("delete from refs where repo =? and name = ? if symbolic =false and object_id = ?")(
        _.bind(repo, name, objectId)).wasApplied()
    }
  }

  def getByName(repo: String, name: String): Option[Ref] = {
    val row = execute("select name,symbolic, object_id, target from refs where repo = ? and name  =? ")(
      _.bind(repo, name)).one()

    Option(row).map(rowToRef)
  }
}
