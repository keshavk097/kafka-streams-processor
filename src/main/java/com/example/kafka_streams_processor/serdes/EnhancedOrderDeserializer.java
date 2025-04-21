package com.example.kafka_streams_processor.serdes;

import com.example.kafka_streams_processor.model.ProcessedStandingOrder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Deserializer;

public class EnhancedOrderDeserializer implements Deserializer<ProcessedStandingOrder> {
    private final ObjectMapper objectMapper;

    public EnhancedOrderDeserializer() {
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule()); // <-- Fix here
    }

    @Override
    public ProcessedStandingOrder deserialize(String topic, byte[] data) {
        try {
            return objectMapper.readValue(data, ProcessedStandingOrder.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
