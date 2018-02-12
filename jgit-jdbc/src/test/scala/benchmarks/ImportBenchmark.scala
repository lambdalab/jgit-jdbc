package benchmarks

import java.io.File

import examples.DaemonExample
import org.eclipse.jgit.api.Git
import org.eclipse.jgit.storage.file.FileRepositoryBuilder


object ImportBenchmark {
  private val SMALL_REPO_URL = "https://github.com/lambdalab/test-repo.git"
  private val MEDIUM_REPO_URL = "https://github.com/lambdalab/javascript-typescript-langserver.git"
  private val LARGE_REPO_URL = "https://github.com/lambdalab/gerrit.git"
  def repoUrl(repo: String) = {
    repo match {
      case "SMALL" => SMALL_REPO_URL
      case "MEDIUM" => MEDIUM_REPO_URL
      case "LARGE" => LARGE_REPO_URL
    }
  }
  def cloneRepo(path: File, remote: String) = {
    try {
      if (!path.exists()) {
        val result = Git.cloneRepository.setURI(remote).setDirectory(path).call
        result.close()
      }
    }
  }

  def clear(remoteName: String, repoName: String): Unit = {
    DaemonExample.clearRepo(s"$remoteName/$repoName")
  }

  def push(localPath: File, remoteName: String, repoName: String) = {
    val repo = new FileRepositoryBuilder()
        .setWorkTree(localPath)
        .build()
    val git = new Git(repo)

    val config = repo.getConfig
    config.setString("remote", remoteName, "url", s"git://localhost/$remoteName/$repoName")
    config.save()

    val push = git.push()
        .add("master")
        .setRemote(remoteName)
        .setForce(true)
    push.call()
  }

  def main(args: Array[String]): Unit = {
    /*DaemonExample.start()
    val parent = new File("/tmp")
    val repo = "MEDIUM"
    val localPath = new File(parent, repo)
    cloneRepo(localPath, MEDIUM_REPO_URL)
    //new TiDBRepoTestBase {}.initJdbc()
    Range(1, 10).foreach { i =>
      println(i)
      clear("mem", repo)
      push(localPath, "mem", repo)
    }*/

  }

}
