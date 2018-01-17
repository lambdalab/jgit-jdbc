package com.lambdalab.jgit.utils

import java.util.concurrent.Callable

import com.google.common.cache.CacheBuilder

class PrepareStatementCache[P <: AnyRef](size: Long) {

  private val cache =
    CacheBuilder
        .newBuilder
        .maximumSize(size)
        .build[Integer, P]()

  def apply(stmt: String)(prepare: String => P) =
    cache.get(
      stmt.hashCode,
      new Callable[P] {
        override def call = prepare(stmt)
      }
    )
}