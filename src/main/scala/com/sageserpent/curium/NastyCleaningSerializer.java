package com.sageserpent.curium;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.ClosureSerializer;

public class NastyCleaningSerializer extends ClosureSerializer {
    @Override
    public void write(Kryo kryo, Output output, Object object) {
        super.write(kryo, output, StolenClosureCleaner.clean(object));
    }
}
