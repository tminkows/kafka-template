package com.example.kafkatemplate.listener;

import com.example.kafkatemplate.schema.CustomerNotification;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@Component
public class CustomerNotificationListener {

    private static final Logger log = LoggerFactory.getLogger(CustomerNotificationListener.class);

    private final AtomicReference<CustomerNotification> lastNotification = new AtomicReference<>();
    private final AtomicReference<CountDownLatch> latch = new AtomicReference<>(new CountDownLatch(1));

    @KafkaListener(topics = "${app.kafka.topic}")
    public void handle(@Payload @Valid @NotNull CustomerNotification notification) {
        log.info("Processing notification {} for customer {}", notification.getId(), notification.getCustomerId());
        lastNotification.set(notification);
        latch.get().countDown();
    }

    public CustomerNotification awaitLastNotification(long timeout, TimeUnit unit) throws InterruptedException {
        CountDownLatch currentLatch = latch.get();
        boolean completed = currentLatch.await(timeout, unit);
        if (!completed) {
            throw new IllegalStateException("Timed out waiting for customer notification");
        }
        return lastNotification.get();
    }

    public void resetLatch() {
        latch.set(new CountDownLatch(1));
    }
}
