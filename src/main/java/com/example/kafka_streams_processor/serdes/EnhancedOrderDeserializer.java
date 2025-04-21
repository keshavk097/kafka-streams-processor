package com.example.kafka_streams_processor.serdes;

import com.example.kafka_streams_processor.model.ProcessedStandingOrder;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

public class EnhancedOrderDeserializer implements Deserializer<ProcessedStandingOrder> {
    @Override
    public ProcessedStandingOrder deserialize(String topic, byte[] data) {
        try {
            return new ObjectMapper().readValue(data, ProcessedStandingOrder.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
