package com.example.kafka_streams_processor.processor;

import com.example.kafka_streams_processor.model.StandingOrder;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.common.serialization.Serdes;

public class StandingOrderSerde extends Serdes.WrapperSerde<StandingOrder> {

    public StandingOrderSerde() {
        super(new JsonSerializer<>(), new JsonDeserializer<>(StandingOrder.class));
    }

    public static StandingOrderSerde of() {
        return new StandingOrderSerde();
    }
}
