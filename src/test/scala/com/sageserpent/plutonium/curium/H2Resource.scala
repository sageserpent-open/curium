package com.sageserpent.plutonium.curium

import cats.effect.{Resource, SyncIO}

object H2Resource {
  // TODO - create tables in in-memory database and yield the transactor used to do so.
  val transactorResource: Resource[SyncIO, H2Tranches.Transactor] = ???
}
