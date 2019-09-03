package my.testcase;

import brave.Tracing;
import brave.kafka.streams.KafkaStreamsTracing;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.kafka.common.requests.IsolationLevel.READ_COMMITTED;
import static org.apache.kafka.streams.StreamsConfig.EXACTLY_ONCE;

@Slf4j
@Configuration
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;
    private final Tracing tracing;

    @PostConstruct
    void init() {
        KafkaStreamsTracing kafkaStreamsTracing = KafkaStreamsTracing.create(tracing);
        final Topology topology = topology(kafkaStreamsTracing);
        kafkaStreamsTracing.kafkaStreams(topology, kStreamsConfig()).start();
    }

    private Topology topology(KafkaStreamsTracing kafkaStreamsTracing) {
        StreamsBuilder sb = new StreamsBuilder();
        KStream<String, byte[]> stream = sb.stream("supplier-test-case-input");
        stream.transformValues(kafkaStreamsTracing
                .valueTransformer("filter", new ValueTransformer<byte[], byte[]>() {

                    private ProcessorContext context;

                    @Override
                    public void init(ProcessorContext context) {
                        this.context = context;
                    }

                    @Override
                    public byte[] transform(byte[] value) {
                        // throws IllegalStateException
                        context.headers();
                        return null;
                    }

                    @Override
                    public void close() {
                    }
                }))
                .to("supplier-test-case-output");
        return sb.build();
    }

    Properties kStreamsConfig() {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-test-case");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "streams-test-case");
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, EXACTLY_ONCE);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "streams-test-case");
        props.put(ConsumerConfig.DEFAULT_ISOLATION_LEVEL, READ_COMMITTED);

        Properties properties = new Properties();
        properties.putAll(props);
        return properties;
    }
}