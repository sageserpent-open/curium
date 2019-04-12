package com.sageserpent.plutonium.curium

import cats.effect.{Resource, IO}

object H2Resource {
  // TODO - create tables in in-memory database and yield the transactor used to do so.
  val transactorResource: Resource[IO, H2Tranches.Transactor] = ???
}
