package tests.ignite

import java.util.Collections

import com.lambdalab.jgit.JGitRepoManager
import com.lambdalab.jgit.ignite.{IgniteRepo, IgniteRepoManager}
import org.apache.ignite.{Ignite, Ignition}
import org.apache.ignite.configuration.{DataRegionConfiguration, DataStorageConfiguration, IgniteConfiguration}
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi

trait IgniteTestBase {
  var dfsRepo: IgniteRepo = _

  protected def repoManager(): JGitRepoManager[IgniteRepo] = {
    val cfg = new IgniteConfiguration()
    cfg.setClientMode(true)
    val spi = new TcpDiscoverySpi
    val finder = new TcpDiscoveryVmIpFinder()
    finder.setAddresses(Collections.singleton("127.0.0.1"))
    spi.setIpFinder(finder)
    cfg.setDiscoverySpi(spi)
    //    cfg.setPeerClassLoadingEnabled(true)
    new IgniteRepoManager(cfg)
  }

  val port = 9042

  var ignite: Ignite = _

  def start(): Unit = {
    val cfg = new IgniteConfiguration()
    val storageCfg = new DataStorageConfiguration()
    storageCfg.setStoragePath("/tmp/ignite")
    storageCfg.getDefaultDataRegionConfiguration().setPersistenceEnabled(true)
    cfg.setDataStorageConfiguration( storageCfg)

    ignite = Ignition.start(cfg)
  }

  def stop(): Unit = {
    // DockerTool.stopContainer(container)
    Ignition.stop(true)
  }
}

object IgiteTestBase extends IgniteTestBase {
  def main(args: Array[String]): Unit = {
    start()
  }
}