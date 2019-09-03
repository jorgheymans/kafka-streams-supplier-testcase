package my.testcase;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class KafkaSender {

    private final KafkaTemplate kafkaTemplate;

    // give enough time for the stream to start
    @Scheduled(fixedDelay = 15000, initialDelay = 15000)
    void sendMessage() {
        kafkaTemplate.send("supplier-test-case-input", "abc".getBytes());
    }
}
