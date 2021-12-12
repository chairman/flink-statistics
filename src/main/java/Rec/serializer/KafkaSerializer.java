package Rec.serializer;

import Rec.KafkaMessage;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.util.Preconditions;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class KafkaSerializer implements KeyedDeserializationSchema<KafkaMessage>, SerializationSchema<KafkaMessage> {
    private static final long serialVersionUID = 1l;
    private transient Charset charset;

    public KafkaSerializer() {
        this(StandardCharsets.UTF_8);
    }

    public KafkaSerializer(Charset character) {
        this.charset = (Charset) Preconditions.checkNotNull(character);
    }

    public Charset getCharset() {
        return charset;
    }

    public void setCharset(Charset charset) {
        this.charset = charset;
    }

    @Override
    public byte[] serialize(KafkaMessage element) {
        return new byte[0];
    }

    @Override
    public KafkaMessage deserialize(byte[] bytes, byte[] bytes1, String s, int i, long l) throws IOException {
        return null;
    }

    @Override
    public boolean isEndOfStream(KafkaMessage kafkaMessage) {
        return false;
    }

    @Override
    public TypeInformation<KafkaMessage> getProducedType() {
        return null;
    }
}
