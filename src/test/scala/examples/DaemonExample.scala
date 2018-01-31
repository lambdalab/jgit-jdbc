package examples

import java.net.InetSocketAddress

import com.lambdalab.jgit.cassandra.CassandraRepoBuilder
import org.eclipse.jgit.lib.Repository
import org.eclipse.jgit.transport.{Daemon, DaemonClient}
import org.eclipse.jgit.transport.resolver.RepositoryResolver

object DaemonExample extends RepositoryResolver[DaemonClient]{
  override def open(req: DaemonClient, name: String): Repository = {
    new CassandraRepoBuilder()
        .setRepoName(name)
        .setKeyspace("jgit")
        .configCluster(_.addContactPoint("127.0.0.1"))
        .build()
  }

  def main(args: Array[String]): Unit = {
    val server = new Daemon(new InetSocketAddress(Daemon.DEFAULT_PORT))
    server.getService("git-receive-pack").setEnabled(true)
    server.setRepositoryResolver(this)
    server.start()
  }

}
