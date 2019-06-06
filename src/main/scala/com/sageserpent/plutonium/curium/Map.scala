package com.sageserpent.plutonium.curium

import com.sageserpent.plutonium.Split
import com.sageserpent.plutonium.curium.ScuzzyMap.InternalKey

import collection.immutable.{Map => StandardMap}
import de.sciss.fingertree.{OrderedSeq => ScissOrderedSeq}

object ScuzzyMap {
  type InternalKey = Split[Int]

  def empty[Key, Value] =
    ScuzzyMap(ScissOrderedSeq.empty[(Key, Value), InternalKey]({
      case (key, _) => Split.alignedWith(key.hashCode())
    }, Ordering[Split[Int]]))
}

case class ScuzzyMap[Key, Value](
    underlying: ScissOrderedSeq[(Key, Value), InternalKey])
    extends StandardMap[Key, Value] {
  override def +[V1 >: Value](kv: (Key, V1)): ScuzzyMap[Key, V1] =
    this.copy(
      underlying = underlyingWithoutKey(kv._1)
        .asInstanceOf[ScissOrderedSeq[(Key, V1), InternalKey]] // Yes, I know; get over it.
        + kv)

  override def get(key: Key): Option[Value] = {
    val lowerBound: InternalKey = Split.lowerBoundOf(key.hashCode())
    val iterator                = underlying.ceilIterator(lowerBound)

    iterator
      .takeWhile {
        case (storedKey, _) => key.hashCode() == storedKey.hashCode()
      }
      .collectFirst { case (storedKey, value) if key == storedKey => value }
  }

  override def iterator: Iterator[(Key, Value)] = underlying.iterator

  override def -(key: Key): ScuzzyMap[Key, Value] = {
    this.copy(underlying = underlyingWithoutKey(key))
  }

  def underlyingWithoutKey(
      key: Key): ScissOrderedSeq[(Key, Value), InternalKey] = {
    underlying
      .get(Split.alignedWith(key.hashCode()))
      .fold(underlying) { keyValuePairToDrop =>
        val lowerBound: InternalKey = Split.lowerBoundOf(key.hashCode())
        val iterator                = underlying.ceilIterator(lowerBound)

        iterator
          .takeWhile {
            case (storedKey, _) => key.hashCode() == storedKey.hashCode()
          }
          .filter { case (storedKey, _) => storedKey != key }
          .foldLeft(underlying.removeAll(keyValuePairToDrop))(_ + _)
      }
  }
}
