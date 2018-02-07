package benchmarks;

import examples.DaemonExample;
import org.apache.commons.lang.math.RandomUtils;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevTree;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.treewalk.TreeWalk;
import org.openjdk.jmh.annotations.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@State(Scope.Benchmark)
public class ReadFileBenchmark {
  @Param({"file", "cassandra", "postgres","tidb","mysql","mem"})
  String engine;

  @Param({"MEDIUM"})
  String repoName;
  private Ref head;
  private Repository repository;

  File getPath() {
    return new File("/tmp", repoName);
  }

  ArrayList<String> files = new ArrayList<>();

  @Setup
  public void setup() throws IOException {
    InitJdbc.init();
    DaemonExample.start();
    ImportBenchmark.cloneRepo(getPath(), ImportBenchmark.repoUrl(repoName));
    //ImportBenchmark.clear(engine, repoName);
    ImportBenchmark.push(getPath(), engine, repoName);
    repository = DaemonExample.openRepo(engine + "/" + repoName);
    head = repository.exactRef("refs/heads/master");
    try (RevWalk walk = new RevWalk(repository)) {
      RevCommit commit = walk.parseCommit(head.getObjectId());
      RevTree tree = walk.parseTree(commit.getTree().getId());
      TreeWalk treeWalk = new TreeWalk(repository);
      treeWalk.addTree(tree);
      treeWalk.setRecursive(true);
      while (treeWalk.next()) {
        files.add(treeWalk.getPathString());
      }
      treeWalk.close();
    }
    System.gc();
  }

  @TearDown
  public void teardown() {
    DaemonExample.stop();
  }

  @Benchmark
  public String readFile() throws IOException {
    try (Repository repository = getRepo();
         RevWalk walk = new RevWalk(repository)) {
      String file = files.get(RandomUtils.nextInt(files.size()));
      RevCommit commit = walk.parseCommit(head.getObjectId());
      RevTree tree = walk.parseTree(commit.getTree().getId());
      TreeWalk treeWalk = TreeWalk.forPath(repository, file, tree);
      byte[] bytes = repository.open(treeWalk.getObjectId(0)).getBytes();
      String content = new String(bytes);
      walk.dispose();
      return content;
    }

  }

  private Repository getRepo() {
    if (engine.equals("mem")) return this.repository;

    return DaemonExample.open(engine + "/" + repoName, false);
  }

  public static void main(String[] args) throws IOException {
    ReadFileBenchmark p = new ReadFileBenchmark();
    p.engine = "mysql";
    p.repoName = "MEDIUM";
    p.setup();
    String content = p.readFile();
    System.out.println("content = " + content);
    System.out.println("content = " + p.readFile());
    p.teardown();
  }
}
