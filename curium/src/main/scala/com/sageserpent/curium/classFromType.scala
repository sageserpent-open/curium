package com.sageserpent.curium

import com.github.benmanes.caffeine.cache.Cache

import scala.reflect.runtime.universe.typeOf
import scala.reflect.runtime.{currentMirror, universe}

object classFromType {
  val clazzCache: Cache[universe.Type, Class[_]] =
    caffeineBuilder().build()

  def apply[Item](reflectedType: universe.Type): Class[Item] =
    clazzCache
      .get(reflectedType, { reflectedType =>
        if (typeOf[Any] =:= reflectedType) classOf[Any]
        else
          currentMirror
            .runtimeClass(reflectedType)
      })
      .asInstanceOf[Class[Item]]
}
