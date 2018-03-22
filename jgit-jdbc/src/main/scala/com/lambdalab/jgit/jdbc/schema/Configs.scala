package com.lambdalab.jgit.jdbc.schema

import scalikejdbc._

case class TextConfig(repo: String, config: String)

trait Configs extends SQLSyntaxSupport[TextConfig] {
  self: JdbcSchemaSupport =>
  override val tableName = self.configsTableName
  val repoName: String

  def apply(r: ResultName[TextConfig])(rs: WrappedResultSet): TextConfig =
    TextConfig(repo = rs.string(r.repo),
      config = rs.string(r.config))
  def apply(r: SyntaxProvider[TextConfig])(rs: WrappedResultSet): TextConfig = apply(r.resultName)(rs)

  def clear(implicit session: DBSession): Unit = {
    withSQL {
      delete.from(this).where(sqls.eq(this.column.repo , repoName))
    }.execute().apply()
  }
  
  def saveText(config: String)(implicit session: DBSession) = {
    val sql = getUpsertSql(tableName,
      "repo,config",
      "{repo}, {config}",
      "config = {config}"
    )

    SQL(sql).bindByName(
      'repo -> repoName,
      'config -> config
    ).update().apply()
  }

  def loadText(implicit session: DBSession) = {
    val r = this.syntax("r")
    withSQL {
      select.from(this as r).where(sqls.eq(r.repo, repoName))
    }.map(this(r)).single().apply().map(_.config)
  }
}
