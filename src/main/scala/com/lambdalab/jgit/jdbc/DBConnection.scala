package com.lambdalab.jgit.jdbc

import scalikejdbc.NamedDB

trait DBConnection {
  def db: NamedDB
}
