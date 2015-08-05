package com.rackspacecloud.blueflood.io.serializers;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;

import java.nio.ByteBuffer;

public class DatastaxSerializer {

    public static class RawSerializer{
        public static ByteBuffer serialize(Object value) {
            DataType datatype = DataType.cdouble();
            return datatype.serialize(value, ProtocolVersion.V3);
        }

        public static Object deserialize(ByteBuffer value) {
            DataType datatype = DataType.cdouble();
            return datatype.deserialize(value, ProtocolVersion.V3);
        }
    }

}
