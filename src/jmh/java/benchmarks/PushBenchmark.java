package benchmarks;

import examples.DaemonExample;
import org.openjdk.jmh.annotations.*;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 5, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@State(Scope.Benchmark)
public class PushBenchmark {

  @Param({"SMALL", "MEDIUM"})
  String repoName;

  @Param({"mem", "file", "cassandra", "mysql", "tidb"})
  String engine;

  File getPath() {
    return new File("/tmp", repoName);
  }


  @Setup
  public void setup() {
    InitJdbc.init();
    DaemonExample.start();
    ImportBenchmark.cloneRepo(getPath(), ImportBenchmark.repoUrl(repoName));
    System.gc();
  }

  @TearDown
  public void teardown() {
    DaemonExample.stop();
  }

  @Benchmark
  public void pushToRepo() {
    ImportBenchmark.clear(engine, repoName);
    ImportBenchmark.push(getPath(), engine, repoName);
  }


  public static void main(String[] args) throws IOException {
    PushBenchmark p = new PushBenchmark();
    p.engine = "cassandra";
    p.repoName = "MEDIUM";
    p.setup();
    p.pushToRepo();
    //p.teardown();
  }

}
