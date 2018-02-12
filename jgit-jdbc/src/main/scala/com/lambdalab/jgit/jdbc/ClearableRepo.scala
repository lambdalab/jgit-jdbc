package com.lambdalab.jgit.jdbc

trait ClearableRepo {
  def clearRepo(init: Boolean= true): Unit
}