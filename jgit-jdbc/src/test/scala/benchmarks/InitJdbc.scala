package benchmarks

import com.lambdalab.jgit.jdbc.test.{MysqlRepoTestBase, PostgresTestBase, TiDBRepoTestBase}
import scalikejdbc.{GlobalSettings, LoggingSQLAndTimeSettings}

object InitJdbc {
  def init(): Unit = {
    GlobalSettings.loggingSQLAndTime = LoggingSQLAndTimeSettings(enabled = false)
    new MysqlRepoTestBase {}.initJdbc()
    new TiDBRepoTestBase {}.initJdbc()
//    new PostgresTestBase {}.initJdbc()
  }
}