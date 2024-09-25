package com.sageserpent.curium

import com.esotericsoftware.kryo.kryo5.objenesis.instantiator.ObjectInstantiator
import com.esotericsoftware.kryo.kryo5.objenesis.strategy.StdInstantiatorStrategy
import com.esotericsoftware.kryo.kryo5.serializers.ClosureSerializer.Closure
import com.esotericsoftware.kryo.kryo5.{Kryo, KryoCopyable}
import com.github.benmanes.caffeine.cache.Cache
import net.bytebuddy.ByteBuddy
import net.bytebuddy.implementation.bind.annotation.{FieldValue, Pipe, RuntimeType}

object ProxySupport {
  case class SuperClazzAndInterfaces(
      superClazz: Class[_],
      interfaces: Seq[Class[_]]
  )

  val superClazzAndInterfacesCache
      : Cache[Class[_], Option[SuperClazzAndInterfaces]] =
    caffeineBuilder().build()
  val cachedProxyClassInstantiators
      : Cache[SuperClazzAndInterfaces, ObjectInstantiator[_]] =
    caffeineBuilder().build()
  val proxiedClazzCache: Cache[Class[_], Class[_]] = caffeineBuilder().build()
}

protected trait ProxySupport {
  type PipeForwarding = Function[AnyRef, Nothing]
  val byteBuddy = new ByteBuddy()
  /* This is tracked to workaround Kryo leaking its internal fudge as to how it
   * registers closure serializers into calls on the tranche specific reference
   * resolver class' methods. */
  val kryoClosureMarkerClazz = classOf[Closure]
  val stateAcquisitionClazz  = classOf[StateAcquisition]
  val kryoCopyableClazz      = classOf[KryoCopyable[StateAcquisition]]
  val instantiatorStrategy: StdInstantiatorStrategy =
    new StdInstantiatorStrategy

  def isProxyClazz(clazz: Class[_]): Boolean =
    stateAcquisitionClazz.isAssignableFrom(clazz)

  def isProxy(immutableObject: AnyRef): Boolean =
    stateAcquisitionClazz.isInstance(immutableObject)

  trait AcquiredState {
    def underlying: AnyRef
  }

  private[curium] trait StateAcquisition {
    def acquire(acquiredState: AcquiredState): Unit
  }

  object proxyDelayedLoading {
    @RuntimeType
    def apply(
        @Pipe pipeTo: PipeForwarding,
        @FieldValue("acquiredState") acquiredState: AcquiredState
    ): Any = {
      val underlying: AnyRef = acquiredState.underlying

      pipeTo(underlying)
    }
  }

  object proxyCopying {
    @RuntimeType
    def copy(
        @RuntimeType kryo: Kryo,
        @FieldValue("acquiredState") acquiredState: AcquiredState
    ): Any = {
      val underlying: AnyRef = acquiredState.underlying

      val copyOfUnderlying = kryo.copy(underlying)
      kryo.reference(copyOfUnderlying)

      copyOfUnderlying
    }
  }

}
