package com.example.kafka_streams_processor.processor;

import com.example.kafka_streams_processor.model.ProcessedStandingOrder;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.errors.StreamsException;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

public class ProcessedStandingOrderSerde extends Serdes.WrapperSerde<ProcessedStandingOrder> {

    public ProcessedStandingOrderSerde() {
        super(new JsonSerializer<>(), new JsonDeserializer<>(ProcessedStandingOrder.class));
    }
}
