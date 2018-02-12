package com.lambdalab.jgit;

import com.google.gerrit.reviewdb.client.Project;
import com.google.gerrit.server.git.RepositoryCaseMismatchException;
import com.google.gerrit.server.git.backends.DfsRepositoryManager;
import org.eclipse.jgit.errors.RepositoryNotFoundException;
import org.eclipse.jgit.internal.storage.dfs.DfsRepository;
import org.eclipse.jgit.lib.Repository;

import java.io.IOException;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

public abstract class JGitRepositoryManager<T extends DfsRepository> extends DfsRepositoryManager {

  protected  JGitRepoManager<T> repoManager;

  @Override
  public Repository openRepository(Project.NameKey name) throws RepositoryNotFoundException, IOException {
    if (!repoManager.isRepoExists(name.get())) {
      throw new RepositoryNotFoundException(name.get());
    }
    return repoManager.openRepo(name.get());
  }

  @Override
  public Repository createRepository(Project.NameKey name) throws RepositoryCaseMismatchException, RepositoryNotFoundException, IOException {
    return repoManager.createRepo(name.get());
  }

  @Override
  public SortedSet<Project.NameKey> list() {
    SortedSet<Project.NameKey> allProjects = new TreeSet<>();
    Iterator<String> it = repoManager.allRepoNames();
    while (it.hasNext()) {
      allProjects.add(Project.NameKey.parse(it.next()));
    }
    return allProjects;
  }

}
