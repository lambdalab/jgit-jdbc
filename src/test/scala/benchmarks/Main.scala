package benchmarks

import com.google.caliper.runner.CaliperMain
import com.lambdalab.jgit.jdbc.test.{MysqlRepoTestBase, TiDBRepoTestBase}
import examples.DaemonExample
import scalikejdbc.{GlobalSettings, LoggingSQLAndTimeSettings}

object Main {

  private def initMysql() = {
    new MysqlRepoTestBase {}.initJdbc()
  }

  private def initTidb() = {
    new TiDBRepoTestBase {}.initJdbc()
  }

  def main(args: Array[String]): Unit = {
    GlobalSettings.loggingSQLAndTime = LoggingSQLAndTimeSettings(
      enabled = false
    )
    initMysql()
    initTidb()
    DaemonExample.start()
    CaliperMain.main(classOf[ImportBenchmark],args)
    DaemonExample.stop()
  }
}
