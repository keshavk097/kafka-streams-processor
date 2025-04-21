package com.example.kafka_streams_processor.serdes;

import com.example.kafka_streams_processor.model.StandingOrder;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

public class OrderSerializer implements Serializer<StandingOrder> {
    @Override
    public byte[] serialize(String topic, StandingOrder data) {
        try {
            return new ObjectMapper().writeValueAsBytes(data);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
