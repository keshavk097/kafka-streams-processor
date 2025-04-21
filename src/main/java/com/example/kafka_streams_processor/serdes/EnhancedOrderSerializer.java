package com.example.kafka_streams_processor.serdes;

import com.example.kafka_streams_processor.model.ProcessedStandingOrder;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

public class EnhancedOrderSerializer implements Serializer<ProcessedStandingOrder> {
    @Override
    public byte[] serialize(String topic, ProcessedStandingOrder data) {
        try {
            return new ObjectMapper().writeValueAsBytes(data);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
