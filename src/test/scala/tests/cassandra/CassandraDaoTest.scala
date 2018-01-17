package tests.cassandra

import java.nio.ByteBuffer

import com.datastax.driver.core.Cluster
import com.lambdalab.jgit.cassandra.{CassandraContext, CassandraPacks}
import org.junit.{Before, Test}
import org.junit.Assert._

class CassandraDaoTest {

  val packs= new CassandraPacks with CassandraContext {
    override val cluster: Cluster = Cluster.builder()
        .addContactPoint("127.0.0.1")
        .build()
    override val keyspace: String = "jgit"
  }
  val repoName = "test"

  @Before
  def setup(): Unit ={
    packs.clear()
  }

  @Test
  def testPacks(): Unit = {
    val newPack = packs.insertNew(repoName,"from " + repoName)
    assertNotNull(newPack)

    val out = packs.writePack(repoName, newPack.id, "idx")
    val wroteString = "write to data"
    val CHUNK_SIZE = 1024 * 100
    val wroteBytes = new Array[Byte](CHUNK_SIZE)
    val len = wroteString.length
    Array.copy(wroteString.getBytes(),0, wroteBytes,0, len)
    out.write(wroteBytes, 0 , wroteBytes.length)
    out.flush()
    out.write(wroteString.getBytes, 0 , len)
    out.flush()

    val readBack = new Array[Byte](len)
    assertEquals(out.read(0,ByteBuffer.wrap(readBack)) , len)
    assertArrayEquals(wroteString.getBytes ,readBack)

    val input = packs.readPack(repoName, newPack.id , "idx")
    input.position(0)
    val readBytes = new Array[Byte](CHUNK_SIZE / 2)
    assertEquals( CHUNK_SIZE / 2, input.read(ByteBuffer.wrap(readBytes)))
    assertArrayEquals(wroteBytes.slice(0 , CHUNK_SIZE /2), readBytes)
    assertEquals( CHUNK_SIZE / 2, input.read(ByteBuffer.wrap(readBytes)))
    assertArrayEquals(wroteBytes.slice(CHUNK_SIZE /2, CHUNK_SIZE), readBytes)

    assertEquals( wroteString.length , input.read(ByteBuffer.wrap(readBytes)))
    assertEquals( new String(readBytes, 0, wroteString.length) , wroteString)
  }

  @Test
  def testPacksBatches(): Unit = {
    val p1 = packs.insertNew(repoName,"1")
    val p2 = packs.insertNew(repoName,"2")
    val p3 = packs.insertNew(repoName,"3")
    val p4 = packs.insertNew(repoName,"4")

    assertTrue("new pack should not committed yet" , packs.allCommitted(repoName).isEmpty)
    assertTrue(packs.commitAll(repoName,Seq(p1.id, p2.id), Seq(p3.id, p4.id)))
    val committed = packs.allCommitted(repoName)
    assertEquals(2 , committed.size)

  }
}
