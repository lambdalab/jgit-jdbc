package benchmarks;

import examples.DaemonExample;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.RefUpdate;
import org.eclipse.jgit.lib.Repository;
import org.openjdk.jmh.annotations.*;

import java.io.File;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 5, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@State(Scope.Benchmark)
public class ChangeRefBenchmark {
  @Param({ "tidb", "mysql",  "cassandra","mem", "file"})
  String engine;

  @Param({"MEDIUM"})
  String repoName;

  File getPath(){
    return new File("/tmp", repoName);
  }

  @Setup
  public void setup() {
    InitJdbc.init();
    DaemonExample.start();
    ImportBenchmark.cloneRepo(getPath(), ImportBenchmark.repoUrl(repoName));
    ImportBenchmark.clear(engine, repoName);
    ImportBenchmark.push(getPath(), engine, repoName);
    System.gc();
  }

  @TearDown
  public void teardown(){
    DaemonExample.stop();
  }

  @Benchmark
  public void  changeRef() {
    Repository repo = DaemonExample.openRepo(engine + "/" + repoName);
    try {
      ObjectId head = repo.resolve("HEAD");
      ObjectId last = repo.resolve("HEAD~1");
      RefUpdate update = repo.updateRef("refs/heads/master");
      update.setNewObjectId(last);
      update.forceUpdate();
      RefUpdate update1 = repo.updateRef("refs/heads/master");
      update1.setNewObjectId(head);
      update1.forceUpdate();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) {
    ChangeRefBenchmark p =new ChangeRefBenchmark();
    p.engine = "mysql";
    p.repoName = "MEDIUM";
    p.setup();
    p.changeRef();
  }
}
