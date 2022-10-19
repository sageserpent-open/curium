package com.sageserpent.curium;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.ClosureSerializer;
import io.findify.flink.api.ClosureCleaner;

// NASTY HACK: this is ghastly, but those Spark, Twitter and Flink folks
// know what they're doing. This prevents closures pulling in lots of
// additional useless objects that are speculatively closed over that
// aren't in general serializable. Once the closure is clean, it can be
// serialized in the usual fashion by the superclass supplied by Kryo.
public class ClosureCleaningSerializer extends ClosureSerializer {
    @Override
    public void write(Kryo kryo, Output output, Object object) {
        super.write(kryo, output, ClosureCleaner.scalaClean(object, false, true));
    }
}
