package com.example.kafka_streams_processor.configuration;

import com.example.kafka_streams_processor.model.ProcessedStandingOrder;
import com.example.kafka_streams_processor.model.StandingOrder;
import com.example.kafka_streams_processor.serdes.EnhancedOrderDeserializer;
import com.example.kafka_streams_processor.serdes.EnhancedOrderSerializer;
import com.example.kafka_streams_processor.serdes.OrderDeserializer;
import com.example.kafka_streams_processor.serdes.OrderSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.LocalDate;
import java.util.Map;

@Configuration
public class KafkaStreamsAppConfig {

    @Bean
    public org.apache.kafka.streams.KafkaStreams kafkaStreams() {
        var builder = new StreamsBuilder();

        var orderSerde = Serdes.serdeFrom(new OrderSerializer(), new OrderDeserializer());
        var enhancedSerde = Serdes.serdeFrom(new EnhancedOrderSerializer(), new EnhancedOrderDeserializer());

        KStream<String, StandingOrder> stream = builder.stream("orders-topic", Consumed.with(Serdes.String(), orderSerde));

        stream.mapValues(order -> {
            ProcessedStandingOrder processed = new ProcessedStandingOrder();
            processed.setOrderId(order.getOrderId());
            processed.setCustomerId(order.getCustomerId());
            processed.setAmount(order.getAmount());
            processed.setAccountNumber(order.getAccountNumber());
            processed.setSortCode(order.getSortCode());
            processed.setStartDate(order.getStartDate());
            processed.setEndDate(order.getEndDate());
            processed.setFrequency(order.getFrequency());
            processed.setStatus(getStatus(order));
            processed.setNextExecutionDate(getNextExecutionDate(order));
            return processed;
        }).to("processed-orders-topic", Produced.with(Serdes.String(), enhancedSerde));

        var props = Map.of(
                StreamsConfig.APPLICATION_ID_CONFIG, "order-stream-processor",
                StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "my-kafka-kafka-bootstrap.kafka.svc:9092"
        );

        var streams = new org.apache.kafka.streams.KafkaStreams(builder.build(), new StreamsConfig(props));
        streams.start();
        return streams;

    }

    private String getStatus(StandingOrder standingOrder) {
        LocalDate today = LocalDate.now();
        if (standingOrder.getStartDate() != null && standingOrder.getEndDate() != null) {
            if (!today.isBefore(standingOrder.getStartDate()) && !today.isAfter(standingOrder.getEndDate())) {
                return "active";
            } else {
                return "inactive";
            }
        }
        return null;
    }

    private LocalDate getNextExecutionDate(StandingOrder standingOrder) {
        LocalDate today = LocalDate.now();
        LocalDate nextExecutionDate = switch (standingOrder.getFrequency().toLowerCase()) {
            case "daily" -> standingOrder.getStartDate().plusDays(1);
            case "weekly" -> standingOrder.getStartDate().plusWeeks(1);
            case "monthly" -> standingOrder.getStartDate().plusMonths(1);
            default -> today;
        };
        return nextExecutionDate;
    }

}
