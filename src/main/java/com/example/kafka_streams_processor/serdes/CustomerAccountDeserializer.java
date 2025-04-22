package com.example.kafka_streams_processor.serdes;

import com.example.kafka_streams_processor.model.CustomerAccount;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

public class CustomerAccountDeserializer implements Deserializer<CustomerAccount> {
    @Override
    public CustomerAccount deserialize(String topic, byte[] data) {
        try {
            return new ObjectMapper().readValue(data, CustomerAccount.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
