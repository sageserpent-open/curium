val sut = com.sageserpent.plutonium.curium.ScuzzyMap.empty[Any, String]

val _1 = sut + (1 -> "Hello")

val _2 = _1 - 1

val _3 = _1 + (2 -> "Goodbye")

val _4 = _1 + (1 -> "Whoah")

_3 - 2

_4.iterator.toList

val _5 = _4 + (1L -> "Cripes")


1L.hashCode()

1.hashCode()

1 == 1L


object onesie{
  override def hashCode(): Int = 1
}

val _6 = _5 + (onesie -> "Twosie")

onesie == 1

_6.size


val _7 = _5.underlyingWithoutKey(onesie) + (onesie -> "Oh")

_7

val _8 = _6 + (56 -> "Old")

_8 - 1

_8 - onesie

_8 - 56





