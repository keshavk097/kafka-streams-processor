package com.example.kafka_streams_processor.serdes;

import com.example.kafka_streams_processor.model.StandingOrder;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

public class OrderDeserializer implements Deserializer<StandingOrder> {
    @Override
    public StandingOrder deserialize(String topic, byte[] data) {
        try {
            return new ObjectMapper().readValue(data, StandingOrder.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

