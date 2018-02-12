import com.lambdalab.jgit.JGitRepoManager;
import com.lambdalab.jgit.jdbc.JdbcRepoManager;
import com.lambdalab.jgit.jdbc.MysqlDfsRepository;
import com.lambdalab.jgit.jdbc.MysqlRepoManager;
import org.eclipse.jgit.dircache.DirCache;
import org.eclipse.jgit.dircache.DirCacheBuilder;
import org.eclipse.jgit.dircache.DirCacheEntry;
import org.eclipse.jgit.errors.IncorrectObjectTypeException;
import org.eclipse.jgit.errors.MissingObjectException;
import org.eclipse.jgit.lib.*;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevTree;
import org.eclipse.jgit.revwalk.RevWalk;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class TestCommit {
  public String repoName="test";
  private MysqlDfsRepository repo;
  String refName="refs/heads/master";
  private RevCommit revision;

  private MysqlRepoManager repoManager() throws ClassNotFoundException {
    Class.forName("com.mysql.jdbc.Driver");
    return JdbcRepoManager.createMysql("jdbc:mysql://localhost:23306/test","root","example");
  }

  @Before
  public void setup() throws ClassNotFoundException, IOException {
    repo = repoManager().openRepo(repoName);
    repo.clearRepo(true);
    Ref ref = repo.getRefDatabase().exactRef(refName);

    try (RevWalk walk = new RevWalk(repo)) {
      revision = ref != null ? walk.parseCommit(ref.getObjectId()) : null;
    }
  }

  protected DirCache readTree(ObjectReader reader, RevTree tree)
      throws IOException, MissingObjectException, IncorrectObjectTypeException {
    DirCache dc = DirCache.newInCore();
    if (tree != null) {
      DirCacheBuilder b = dc.builder();
      b.addTree(new byte[0], DirCacheEntry.STAGE_0, reader, tree);
      b.finish();
    }
    return dc;
  }

  @Test
  public void testCommit() throws IOException {
    ObjectInserter inserter;
    ObjectReader reader;
    try (
        ObjectInserter i = repo.newObjectInserter();
        ObjectReader r = repo.newObjectReader();
        RevWalk rw = new RevWalk(r)) {
      inserter = i;
      reader = r;

      RevTree srcTree = revision != null ? rw.parseTree(revision) : null;
      DirCache newTree = readTree(reader,srcTree);
      PersonIdent ident=new PersonIdent(repo);
      CommitBuilder commit = new CommitBuilder();
      commit.setAuthor(ident);
      commit.setCommitter(ident);
      String msg = "test messages";
      commit.setMessage(msg);

      ObjectId res = newTree.writeTree(inserter);
      if (res.equals(srcTree)) {
        return;
      }
      commit.setTreeId(res);

      ObjectId newRevision = inserter.insert(commit);
      inserter.flush();
      updateRef(ident, newRevision, "commit: " + msg);
      revision = rw.parseCommit(newRevision);
    }
  }

  private void updateRef(PersonIdent ident, ObjectId newRevision, String refLogMsg) throws IOException {
    RefUpdate ru = repo.updateRef(refName);
    ru.setRefLogIdent(ident);
    ru.setNewObjectId(newRevision);
    ru.setExpectedOldObjectId(revision);
    ru.setRefLogMessage(refLogMsg, false);
    RefUpdate.Result r = ru.update();
    switch (r) {
      case FAST_FORWARD:
      case NEW:
      case NO_CHANGE:
        break;
      case FORCED:
      case IO_FAILURE:
      case LOCK_FAILURE:
      case NOT_ATTEMPTED:
      case REJECTED:
      case REJECTED_CURRENT_BRANCH:
      case RENAMED:
      case REJECTED_MISSING_OBJECT:
      case REJECTED_OTHER_REASON:
      default:
        throw new IOException(
            "Failed to update " + refName + " of " + repoName +  ": " + r.name());
    }
  }
}
