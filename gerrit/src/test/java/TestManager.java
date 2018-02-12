
import com.lambdalab.jgit.JGitRepoManager;
import com.lambdalab.jgit.jdbc.JdbcRepoManager;
import com.lambdalab.jgit.jdbc.MysqlRepoManager;
import org.junit.*;
import scalikejdbc.ConnectionPool;

import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class TestManager {
  public String repoName="test";

  @Test
  public void testManager() throws ClassNotFoundException, InterruptedException {
    JGitRepoManager repoManager = repoManager();
    repoManager.init();
    repoManager.deleteRepo(repoName);
    assertFalse("repo should not exists after delete", repoManager.isRepoExists(repoName));
    repoManager.createRepo(repoName);
    assertTrue("repo should exists after create", repoManager.isRepoExists(repoName));
    Iterator<String> it = repoManager.allRepoNames();
    assertTrue(it.hasNext());
    assertEquals(repoName, it.next());
  }

  private JGitRepoManager repoManager() throws ClassNotFoundException {
    Class.forName("com.mysql.jdbc.Driver");
    return JdbcRepoManager.createMysql("jdbc:mysql://localhost:23306/test","root","example");
  }
}
