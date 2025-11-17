package com.example.kafkatemplate;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class KafkaTemplateApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaTemplateApplication.class, args);
    }
}
