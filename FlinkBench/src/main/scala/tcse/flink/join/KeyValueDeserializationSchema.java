package tcse.flink.join;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.io.IOException;

/**
 * Created by SkyDream on 2016/7/6.
 */
public class KeyValueDeserializationSchema implements KeyedDeserializationSchema<Tuple2<String,String>>{

    private SimpleStringSchema sss = new SimpleStringSchema();

    @Override
    public Tuple2<String, String> deserialize(byte[] messageKey, byte[] message, String topic, int partition, long offset) throws IOException {
        String key = sss.deserialize(messageKey);
        String value = sss.deserialize(message);
        return new Tuple2<String,String>(key,value);
    }

    @Override
    public boolean isEndOfStream(Tuple2<String, String> nextElement) {
        return sss.isEndOfStream(nextElement.f0) && sss.isEndOfStream(nextElement.f1);
    }

    @Override
    public TypeInformation<Tuple2<String, String>> getProducedType() {
        return new TupleTypeInfo<Tuple2<String,String>>(BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.STRING_TYPE_INFO);
    }
}
