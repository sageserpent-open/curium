package com.sageserpent.curium

import com.esotericsoftware.kryo.kryo5.Kryo
import com.esotericsoftware.kryo.kryo5.io.{Input, Output}
import com.esotericsoftware.kryo.kryo5.util.Pool

import java.io.ByteArrayOutputStream
import scala.util.Using
import scala.util.Using.Releasable

// Replacement for the now removed use of Chill's `KryoPool`...
class SerializationFacade(
    kryoPool: Pool[Kryo],
    inputPool: Pool[Input],
    outputPool: Pool[Output]
) {
  def evidence[X](pool: Pool[X]): Releasable[X] = pool.free

  implicit val kryoEvidence: Releasable[Kryo]     = evidence(kryoPool)
  implicit val inputEvidence: Releasable[Input]   = evidence(inputPool)
  implicit val outputEvidence: Releasable[Output] = evidence(outputPool)

  def fromBytes(bytes: Array[Byte]): Any =
    Using.resources(kryoPool.obtain(), inputPool.obtain()) { (kryo, input) =>
      input.setBuffer(bytes)
      kryo.readClassAndObject(input)
    }

  def toBytesWithClass(immutableObject: Any): Array[Byte] =
    Using.resources(kryoPool.obtain(), outputPool.obtain()) { (kryo, output) =>
      val byteArrayOutputStream =
        output.getOutputStream.asInstanceOf[ByteArrayOutputStream]
      byteArrayOutputStream.reset()
      output.reset()

      kryo.writeClassAndObject(output, immutableObject)

      output.flush()
      byteArrayOutputStream.toByteArray
    }

  def copy[X](immutableObject: X): X =
    Using.resource(kryoPool.obtain())(_.copy(immutableObject))
}
