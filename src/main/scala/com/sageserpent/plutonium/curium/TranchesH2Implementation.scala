package com.sageserpent.plutonium.curium

import ImmutableObjectStorage.Tranches

abstract class TranchesH2Implementation[Payload]
    extends Tranches[Any, Payload] {} // TODO - make the class concrete and decide on what type the tranche ids should take - what can H2 generate automatically?
