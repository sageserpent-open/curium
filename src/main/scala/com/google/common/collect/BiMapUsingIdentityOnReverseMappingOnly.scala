package com.google.common.collect
import java.util

object BiMapUsingIdentityOnReverseMappingOnly {
  def fromForwardMap[Key, Value](forwardMap: util.Map[Key, Value])
    : BiMapUsingIdentityOnReverseMappingOnly[Key, Value] =
    new BiMapUsingIdentityOnReverseMappingOnly[Key, Value](forwardMap)
}

class BiMapUsingIdentityOnReverseMappingOnly[Key, Value](
    forwardMap: util.Map[Key, Value])
    extends AbstractBiMap[Key, Value](forwardMap,
                                      new util.IdentityHashMap[Value, Key])
