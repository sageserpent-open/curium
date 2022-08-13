package com.google.common.collect

import java.util

object BiMapFactory {
  def usingIdentityForInverse[Key, Value]()
  : BiMap[Key, Value] =
    new AbstractBiMap[Key, Value](new util.HashMap[Key, Value],
      new util.IdentityHashMap[Value, Key]) {}


  def empty[Key, Value]()
  : BiMap[Key, Value] = new AbstractBiMap[Key, Value](new util.HashMap[Key, Value], new util.HashMap[Value, Key]) {}
}

