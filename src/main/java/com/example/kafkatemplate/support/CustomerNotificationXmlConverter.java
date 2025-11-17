package com.example.kafkatemplate.support;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.oxm.MarshallingFailureException;
import org.springframework.oxm.UnmarshallingFailureException;
import org.springframework.oxm.jaxb.Jaxb2Marshaller;

import javax.xml.transform.stream.StreamSource;
import java.io.StringReader;
import java.lang.reflect.Type;

public class CustomerNotificationXmlConverter implements RecordMessageConverter {

    private final Jaxb2Marshaller marshaller;

    public CustomerNotificationXmlConverter(Jaxb2Marshaller marshaller) {
        this.marshaller = marshaller;
    }

    @Override
    public Message<?> toMessage(ConsumerRecord<?, ?> record, Acknowledgment acknowledgment, Consumer<?, ?> consumer, Type type) {
        Object payload = record.value();
        if (payload instanceof String xmlPayload) {
            try {
                Object notification = marshaller.unmarshal(new StreamSource(new StringReader(xmlPayload)));
                return MessageBuilder.withPayload(notification)
                        .setHeader(KafkaHeaders.RECEIVED_TOPIC, record.topic())
                        .setHeader(KafkaHeaders.OFFSET, record.offset())
                        .build();
            } catch (MarshallingFailureException | UnmarshallingFailureException ex) {
                throw ex;
            }
        }
        return MessageBuilder.withPayload(payload).build();
    }

    @Override
    public ConsumerRecord<?, ?> fromMessage(Message<?> message, String defaultTopic) {
        throw new UnsupportedOperationException("XML serialization is not supported for outbound messages in this template");
    }
}
