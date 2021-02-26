package kafka;

import kafka.SimpleMessageProtos.SimpleMessage;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Properties;

public class Producer {
    public static void main(String[] args) {

        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class);
        props.put(KafkaProtobufSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        final KafkaProducer<String, SimpleMessage> producer = new KafkaProducer<>(props);

        SimpleMessage simpleMessage =
                SimpleMessage.newBuilder()
                        .setContent("Hello world")
                        .setDateTime(Instant.now().toString())
                        .build();

        System.out.println(simpleMessage);

        ProducerRecord<String, SimpleMessage> record
                = new ProducerRecord<>("protobuf-topic", null, simpleMessage);

        producer.send(record);
        //ensures record is sent before closing the producer
        producer.flush();

        producer.close();

    }
}
