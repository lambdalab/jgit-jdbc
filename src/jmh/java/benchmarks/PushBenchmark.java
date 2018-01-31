package benchmarks;

import examples.DaemonExample;
import org.openjdk.jmh.annotations.*;

import java.io.File;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 5, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@State(Scope.Benchmark)
public class PushBenchmark {

  @Param({"SMALL", "MEDIUM"})
  String repo;

  @Param({"mem", "file", "mysql", "tidb", "cassandra"})
  String engine;

  File getPath(){
    return new File("/tmp", repo);
  }


  @Setup
  public void setup() {
    InitJdbc.init();
    DaemonExample.start();
    ImportBenchmark.cloneRepo(getPath(), ImportBenchmark.repoUrl(repo));
    System.gc();
  }

  @TearDown
  public void teardown(){
    DaemonExample.stop();
  }

  @Benchmark
  public void  pushToRepo() {
    ImportBenchmark.clear(engine, repo);
    ImportBenchmark.push(getPath(), engine, repo);
  }

}
