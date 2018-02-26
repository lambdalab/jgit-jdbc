package com.lambdalab.jgit.ignite

import org.apache.ignite.Ignite
import org.apache.ignite.transactions.Transaction

trait IgniteTxSupport {
  def ignite:Ignite
  def withTx[T](run: (Transaction) => T): T = {
    val tx  = ignite.transactions().txStart()
    try {
      val ret = run(tx)
      tx.commit()
      ret
    } finally{
      tx.close()
    }
  }
}
