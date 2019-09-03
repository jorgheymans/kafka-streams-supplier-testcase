package my.testcase;

import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.COMPRESSION_TYPE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION;
import static org.apache.kafka.clients.producer.ProducerConfig.RETRIES_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.TRANSACTIONAL_ID_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

@EnableKafka
@Configuration
public class ProducerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    KafkaTemplate<String, byte[]> kafkaTemplate() {
        KafkaTemplate<String, byte[]> template = new KafkaTemplate<>(producerFactory());
        template.setDefaultTopic("supplier-test-case-input");
        return template;
    }

    Map<String, Object> producerConfig() {
        Map<String, Object> props = new HashMap<>();
        props.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(CLIENT_ID_CONFIG, "applicationName");
        props.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        // wait for min.insync.replicas (server side configured) to ACK the message, strongest durability but 'slower'
        props.put(ACKS_CONFIG, "all");
        // avoids messages being sent twice due to broker failures, works on a producer instance basis
        // so if your producer crashes and restarts you can still have dupes
        props.put(ENABLE_IDEMPOTENCE_CONFIG, true);
        // avoids sending messages out of order in case of retries
        props.put(MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        // needed for idempotence
        props.put(RETRIES_CONFIG, 5);
        props.put(TRANSACTIONAL_ID_CONFIG, "applicationName");
        props.put(COMPRESSION_TYPE_CONFIG, "zstd");
        return props;
    }

    @Bean
    ProducerFactory<String, byte[]> producerFactory() {
        DefaultKafkaProducerFactory<String, byte[]> producerFactory = new DefaultKafkaProducerFactory<>(
                producerConfig());
        producerFactory.setTransactionIdPrefix("applicationName");
        return producerFactory;
    }
}