package tests.cassandra

import java.nio.ByteBuffer

import com.lambdalab.jgit.cassandra._
import io.netty.buffer.ByteBuf
import org.eclipse.jgit.internal.storage.dfs.DfsRepositoryDescription
import org.junit.Assert._
import org.junit.{Before, Test}

class CassandraDaoTest extends CassandraTestBase {
  val repoName = "test"


  val packs= new CassandraPacks with CassandraContext {
    override val settings: CassandraSettings = cassandraSettings

    override def loadChunk(id: String, ext: String, chunk: Int): ByteBuf = ???
  }
  val refs = new CassandraRefs with CassandraContext {
    override val settings: CassandraSettings = cassandraSettings
  }

  @Before
  def setup(): Unit ={
    packs.clear(repoName)
    refs.clear(repoName)
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
    val repoDesc=new DfsRepositoryDescription(repoName)
    val p1 = CassandraDfsPackDescription(repoDesc,packs.insertNew(repoName,"1"))
    val p2 = CassandraDfsPackDescription(repoDesc,packs.insertNew(repoName,"2"))
    val p3 = CassandraDfsPackDescription(repoDesc,packs.insertNew(repoName,"3"))
    val p4 = CassandraDfsPackDescription(repoDesc,packs.insertNew(repoName,"4"))

    assertTrue("new pack should not committed yet" , packs.allCommitted(repoName).isEmpty)
    assertTrue(packs.commitAll(repoName,Seq(p1, p2), Seq(p3, p4)))
    val committed = packs.allCommitted(repoName)
    assertEquals(2 , committed.size)
  }

  @Test
  def testRefs(): Unit = {
    val ref = Ref("HEAD", symbolic = true , target = "refs/head/master", objectId = null)
    assertTrue(refs.newRef(repoName, ref))
    assertTrue(refs.all(repoName).contains(ref))

    val wrongOldRef = ref.copy(target = "something else")
    val newRef =  Ref("HEAD", false , null, "objectId")
    assertFalse("should not update if oldref conflict", refs.updateRef(repoName, wrongOldRef, newRef))
    assertTrue("should update if oldref matches", refs.updateRef(repoName, ref, newRef))


    assertFalse("should not delete if oldref conflict", refs.delete(repoName, ref))
    assertTrue(refs.delete(repoName, newRef))
    assertTrue(refs.all(repoName).isEmpty)
  }
}
