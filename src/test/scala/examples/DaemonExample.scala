package examples

import java.io.File
import java.net.InetSocketAddress
import java.util.concurrent.Callable

import com.google.common.cache.CacheBuilder
import com.google.common.io.Files
import com.lambdalab.jgit.cassandra.CassandraRepoBuilder
import com.lambdalab.jgit.jdbc.test.{MysqlRepoTestBase, TiDBRepoTestBase}
import com.lambdalab.jgit.jdbc.{ClearableRepo, MysqlRepoBuilder}
import org.eclipse.jgit.api.Git
import org.eclipse.jgit.internal.storage.dfs.InMemoryRepository
import org.eclipse.jgit.internal.storage.file.FileRepository
import org.eclipse.jgit.lib.Repository
import org.eclipse.jgit.transport.{Daemon, DaemonClient}
import org.eclipse.jgit.transport.resolver.RepositoryResolver
import org.eclipse.jgit.storage.file.FileRepositoryBuilder

object DaemonExample extends RepositoryResolver[DaemonClient] {

  def clearRepo(name: String): Repository = {

    val repo = openRepo(name)
    repo match {
      case r: ClearableRepo =>
        r.clearRepo
        repo
      case f: FileRepository =>
        f.getDirectory.delete()
        reposCache.invalidate(name)
        openRepo(name)
      case _ =>
        repo.close()
        reposCache.invalidate(name)
        openRepo(name)
    }

  }

  val repoParent = Files.createTempDir()
  repoParent.deleteOnExit()

  val reposCache = CacheBuilder.newBuilder().build[String, Repository]()

  override def open(req: DaemonClient, name: String): Repository = {
    reposCache.get(name, new Callable[Repository] {
      override def call(): Repository = openRepo(name)
    })
  }

  def openRepo(name: String) = {
    val Array(engine, repo) = name.split('/')
    engine match {
      case "cassandra" =>
        val r = new CassandraRepoBuilder()
            .setRepoName(repo)
            .setKeyspace("jgit")
            .configCluster(_.addContactPoint("127.0.0.1"))
            .setBare()
            .build()
        if(!r.exists())
          r.create(true)
        r
      case "mysql" =>
        val r = new MysqlRepoBuilder()
            .setRepoName(repo)
            .setDBName('mysql)
            .setBare()
            .build()
        if (!r.exists())
          r.create()
        r
      case "tidb" =>
        val r = new MysqlRepoBuilder()
            .setRepoName(repo)
            .setDBName('tidb)
            .setBare()
            .build()
        if (!r.exists())
          r.create()
        r
      case "file" =>
        val repoDir = new File(repoParent, repo)
        Git.init().setBare(true).setDirectory(repoDir).call().getRepository
      case _ =>
        val r = new InMemoryRepository.Builder()
            .setBare()
            .build()
        r.create(true)
        r
    }
  }

  val server = new Daemon(new InetSocketAddress(Daemon.DEFAULT_PORT))
  server.getService("git-receive-pack").setEnabled(true)
  server.setRepositoryResolver(this)

  def start(): Unit = {
    server.start()
  }

  def stop(): Unit = {
    server.stop()
  }

  def main(args: Array[String]): Unit = {
    new TiDBRepoTestBase {}.initJdbc()
    new MysqlRepoTestBase {}.initJdbc()
    start()
  }
}
