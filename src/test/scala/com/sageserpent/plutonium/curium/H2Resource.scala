package com.sageserpent.plutonium.curium

import resource._

object H2Resource {
  // TODO - create tables in in-memory database and yield the transactor used to do so.
  val transactorResource: ManagedResource[H2Tranches.Transactor] = ???
}
