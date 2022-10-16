package com.sageserpent.curium;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.ClosureSerializer;
import io.findify.flink.api.ClosureCleaner;

// NASTY HACK: this is ghastly, but those Spark, Twitter and Flink folks know what
// they're doing. Just using plain old `ClosureSerializer` will cause a test failure
// due to uncleaned closures pulling in lots of useless objects that are speculatively
// closed over. Yes, we need `ClosureSerializer` from plain Kryo too. TODO: is this
// comment really true? The test for stricture sharing is still failing at time of writing!
public class ClosureCleaningSerializer extends ClosureSerializer {
    @Override
    public void write(Kryo kryo, Output output, Object object) {
        super.write(kryo, output, ClosureCleaner.scalaClean(object, false, true));
    }
}
