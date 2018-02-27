package com.lambdalab.jgit;

import com.google.gerrit.server.git.backends.GitBackendConfig;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.lambdalab.jgit.ignite.IgniteRepo;
import com.lambdalab.jgit.ignite.IgniteRepoManager;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.kubernetes.TcpDiscoveryKubernetesIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import java.util.Arrays;

@Singleton
public class IgniteRepositoryManager extends JGitRepositoryManager<IgniteRepo> {
  private static final String CLIENT_MODE = "clientMode";
  private static final String STORAGE_PATH = "storagePath";
  private static final String IP_FINDER = "ipFinder";
  private static final String K8_FINDER = "k8";

  private GitBackendConfig config;


  @Inject
  public IgniteRepositoryManager(GitBackendConfig config) {
    this.config = config;
    start();
  }

  @Override
  public void stop() {

  }

  @Override
  public void start() {
    if (repoManager != null) return;
    boolean clientMode = "true".equalsIgnoreCase(config.getString(CLIENT_MODE));
    IgniteConfiguration cfg = new IgniteConfiguration();
    cfg.setClientMode(clientMode);
    if (!clientMode) {
      String storagePath = config.getString(STORAGE_PATH);
      if(storagePath!=null) {
        DataStorageConfiguration storageCfg = new DataStorageConfiguration();
        storageCfg.setStoragePath(storagePath);
        storageCfg.getDefaultDataRegionConfiguration().setPersistenceEnabled(true);
        cfg.setDataStorageConfiguration(storageCfg);
      }
    }
    String ipFinder = config.getString(IP_FINDER);
    if(ipFinder!=null) {
      TcpDiscoverySpi spi=new TcpDiscoverySpi();
      if(ipFinder.equals(K8_FINDER)){
        spi.setIpFinder(new TcpDiscoveryKubernetesIpFinder());
      } else {
        String[] addresses = ipFinder.split("[\\s,;]");
        spi.setIpFinder(new TcpDiscoveryVmIpFinder().setAddresses(Arrays.asList(addresses)));
      }
      cfg.setDiscoverySpi(spi);
    }
    repoManager = new IgniteRepoManager(cfg);
    repoManager.init();
  }
}
