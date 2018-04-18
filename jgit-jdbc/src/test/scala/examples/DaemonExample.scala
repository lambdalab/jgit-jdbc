package examples

import java.io.File
import java.net.InetSocketAddress
import java.util.Collections
import java.util.concurrent.Callable

import benchmarks.InitJdbc
import com.google.common.cache.CacheBuilder
import com.google.common.io.Files
import com.lambdalab.jgit.cassandra.CassandraRepoBuilder
import com.lambdalab.jgit.ignite.IgniteRepoBuilder
import com.lambdalab.jgit.jdbc.{ClearableRepo, MysqlRepoBuilder, PostgresRepoBuilder}
import io.insight.jgit.server.grpc.GrpcServer
import io.insigit.jgit.RpcRepository
import io.insigit.jgit.grpc.GrpcClientRepoManager
import org.apache.commons.io.FileUtils
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder
import org.eclipse.jgit.api.Git
import org.eclipse.jgit.internal.storage.dfs.{DfsRepositoryDescription, InMemoryRepository}
import org.eclipse.jgit.internal.storage.file.FileRepository
import org.eclipse.jgit.lib.Repository
import org.eclipse.jgit.transport.resolver.RepositoryResolver
import org.eclipse.jgit.transport.{Daemon, DaemonClient}
import scalikejdbc.ConnectionPool

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
      case f: RpcRepository =>
        grpcRepoManager.delete(name)
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
            .setDBName(ConnectionPool.DEFAULT_NAME)
            .setBare()
            .build()
        if (create && !r.exists())
          r.create()
        r
      case "file" =>
        val repoDir = new File(repoParent, repo)
        if (create)
          Git.init().setBare(true).setDirectory(repoDir).call().getRepository
        else {
          Git.open(repoDir).getRepository
        }
      case "postgres" =>
        val r = new PostgresRepoBuilder()
            .setRepoName(repo)
            .setDBName('postgres)
            .setBare()
            .build()
        if (create && !r.exists())
          r.create()
        r
      case "ignite" =>
        val r = new IgniteRepoBuilder()
            .setRepoName(repo)
            .setupIgnite { cfg =>
              cfg.setClientMode(true)
              val spi = new TcpDiscoverySpi
              val finder = new TcpDiscoveryVmIpFinder()
              finder.setAddresses(Collections.singleton("127.0.0.1"))
              spi.setIpFinder(finder)
              cfg.setDiscoverySpi(spi)
            }
            .build()
        if (create && !r.exists())
          r.create()
        r
      case "grpc" =>
        if(grpcRepoManager.exists(repo))
          grpcRepoManager.open(repo)
        else
          grpcRepoManager.create(repo)
      case _ =>
        new InMemoryRepository(new DfsRepositoryDescription(repo))

    }
  }
  val grpcRepoManager = new GrpcClientRepoManager("localhost", 10000)
  val grpcServer = new GrpcServer(10000, new File("/tmp/grpc"))
  val server = new Daemon(new InetSocketAddress(Daemon.DEFAULT_PORT))
  server.getService("git-receive-pack").setEnabled(true)
  server.setRepositoryResolver(this)

  def start(): Unit = {
    server.start()
//    grpcServer.start()
  }

  def stop(): Unit = {
//    grpcServer.stop()
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
