package com.example.kafkatemplate;

import com.example.kafkatemplate.listener.CustomerNotificationListener;
import com.example.kafkatemplate.schema.CustomerNotification;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = "my-topic")
@TestPropertySource(properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class CustomerNotificationListenerIT {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private CustomerNotificationListener listener;

    @BeforeEach
    void setUp() {
        listener.resetLatch();
    }

    @Test
    void shouldConsumeCustomerNotification() throws Exception {
        String payload = buildPayload();

        kafkaTemplate.send("my-topic", payload).get(5, TimeUnit.SECONDS);

        CustomerNotification notification = listener.awaitLastNotification(5, TimeUnit.SECONDS);

        assertThat(notification).isNotNull();
        assertThat(notification.getMessage()).isEqualTo("Your order has shipped");
    }

    private String buildPayload() {
        String id = UUID.randomUUID().toString();
        String customerId = UUID.randomUUID().toString();
        String createdAt = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(OffsetDateTime.now());
        return """
                <customerNotification xmlns=\"http://example.com/kafka/customer-notification\">
                    <id>%s</id>
                    <customerId>%s</customerId>
                    <createdAt>%s</createdAt>
                    <channel>EMAIL</channel>
                    <message>Your order has shipped</message>
                </customerNotification>
                """.formatted(id, customerId, createdAt);
    }
}
