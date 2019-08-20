package com.sageserpent.curium

import com.github.benmanes.caffeine.cache.Caffeine

object caffeineBuilder {
  def apply(): Caffeine[Any, Any] =
    // HACK: have to workaround the freezing of the nominal type parameters
    // to [AnyRef, AnyRef] in the Caffeine library code. Although it is a lie,
    // the instance created as only used as a springboard to building a cache;
    // the type parameters don't matter other than as arbitrary constraints on
    // what kind of cache can be built.
    Caffeine.newBuilder.asInstanceOf[Caffeine[Any, Any]]
}
