package com.lambdalab.jgit;

import com.google.gerrit.server.git.backends.GitBackendConfig;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.lambdalab.jgit.jdbc.JdbcRepoManager;
import com.lambdalab.jgit.jdbc.MysqlDfsRepository;
import com.lambdalab.jgit.jdbc.MysqlRepoManager;

@Singleton
public class MysqlRepositoryManager extends JGitRepositoryManager<MysqlDfsRepository>{

  private GitBackendConfig config;

  @Inject
  public MysqlRepositoryManager(GitBackendConfig config) {
    this.config = config;
    start();
  }

  @Override
  public void stop() {

  }

  @Override
  public void start() {
    if(repoManager !=null) return;
    try {
      Class.forName("com.mysql.jdbc.Driver");
      repoManager = JdbcRepoManager.createMysql(config.getString("url"), config.getString("user"), config.getString("password"));
      repoManager.init();
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }
}
