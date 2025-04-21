package com.example.kafka_streams_processor.serdes;

import com.example.kafka_streams_processor.model.StandingOrder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Deserializer;

public class OrderDeserializer implements Deserializer<StandingOrder> {
    private final ObjectMapper objectMapper;

    public OrderDeserializer() {
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule()); // <-- Fix here
    }

    @Override
    public StandingOrder deserialize(String topic, byte[] data) {
        try {
            return objectMapper.readValue(data, StandingOrder.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

