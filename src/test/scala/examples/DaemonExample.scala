package examples

import java.io.File
import java.net.InetSocketAddress
import java.util.concurrent.Callable

import benchmarks.InitJdbc
import com.google.common.cache.CacheBuilder
import com.google.common.io.Files
import com.lambdalab.jgit.cassandra.CassandraRepoBuilder
import com.lambdalab.jgit.jdbc.{ClearableRepo, MysqlRepoBuilder}
import org.apache.commons.io.FileUtils
import org.apache.commons.lang.math.RandomUtils
import org.eclipse.jgit.api.Git
import org.eclipse.jgit.internal.storage.dfs.{DfsRepositoryDescription, InMemoryRepository}
import org.eclipse.jgit.internal.storage.file.FileRepository
import org.eclipse.jgit.lib.Repository
import org.eclipse.jgit.transport.resolver.RepositoryResolver
import org.eclipse.jgit.transport.{Daemon, DaemonClient}

object DaemonExample extends RepositoryResolver[DaemonClient] {

  def clearRepo(name: String): Repository = {

    val repo = openRepo(name)
    repo match {
      case r: ClearableRepo =>
        r.clearRepo()
        repo
      case f: FileRepository =>
//        f.close()
        FileUtils.deleteQuietly(f.getDirectory)

        reposCache.invalidate(name)
        openRepo(name)
      case _ =>
        //        repo.close()
        reposCache.invalidate(name)
        openRepo(name)
    }

  }

  val repoParent = Files.createTempDir()
  repoParent.deleteOnExit()

  val reposCache = CacheBuilder.newBuilder()
      .build[String, Repository]()

  override def open(req: DaemonClient, name: String): Repository = {
    reposCache.get(name, new Callable[Repository] {
      override def call(): Repository = open(name)
    })
  }

  def openRepo(name: String) = {
    reposCache.get(name, new Callable[Repository] {
      override def call(): Repository = open(name)
    })
  }

  var cassandraStarted = false
  lazy val cassandraBuilder = {
    cassandraStarted = true
    new CassandraRepoBuilder()
        .setKeyspace("jgit")
        .configCluster(_.addContactPoint("127.0.0.1"))
  }

  def open(name: String, create: Boolean = true) = {
    val Array(engine, repo) = name.split('/')
    engine match {
      case "cassandra" =>
        val r = cassandraBuilder
            .setRepoName(repo)
            .setBare()
            .build()
        if (create && !r.exists())
          r.create(true)
        r
      case "mysql" =>
        val r = new MysqlRepoBuilder()
            .setRepoName(repo)
            .setDBName('mysql)
            .setBare()
            .build()
        if (create && !r.exists())
          r.create()
        r
      case "tidb" =>
        val r = new MysqlRepoBuilder()
            .setRepoName(repo)
            .setDBName('tidb)
            .setBare()
            .build()
        if (create && !r.exists())
          r.create()
        r
      case "file" =>
        val repoDir = new File(repoParent, repo + "_" + RandomUtils.nextInt())
        if (create)
          Git.init().setBare(true).setDirectory(repoDir).call().getRepository
        else {
          Git.open(repoDir).getRepository
        }
      case _ =>
        new InMemoryRepository(new DfsRepositoryDescription(repo))

    }
  }

  val server = new Daemon(new InetSocketAddress(Daemon.DEFAULT_PORT))
  server.getService("git-receive-pack").setEnabled(true)
  server.setRepositoryResolver(this)

  def start(): Unit = {
    server.start()
  }

  def stop(): Unit = {

    reposCache.invalidateAll()
    if (cassandraStarted)
      cassandraBuilder.close()
    server.stop()
  }

  def main(args: Array[String]): Unit = {
    InitJdbc.init()
    start()
  }
}
