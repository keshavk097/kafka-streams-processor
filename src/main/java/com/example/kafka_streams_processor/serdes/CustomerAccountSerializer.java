package com.example.kafka_streams_processor.serdes;

import com.example.kafka_streams_processor.model.CustomerAccount;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

public class CustomerAccountSerializer implements Serializer<CustomerAccount> {

    @Override
    public byte[] serialize(String topic, CustomerAccount data) {
        try {
            return new ObjectMapper().writeValueAsBytes(data);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
