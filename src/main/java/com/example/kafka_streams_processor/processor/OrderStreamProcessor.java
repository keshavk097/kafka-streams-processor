package com.example.kafka_streams_processor.processor;

import com.example.kafka_streams_processor.model.ProcessedStandingOrder;
import com.example.kafka_streams_processor.model.StandingOrder;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.stereotype.Component;

import java.time.LocalDate;

@EnableKafkaStreams
@Component
public class OrderStreamProcessor {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Bean
    public KStream<String, StandingOrder> processOrders(StreamsBuilder builder) {
        // Create a custom Serde for StandingOrder
        StandingOrderSerde standingOrderSerde = StandingOrderSerde.of();

        KStream<String, StandingOrder> stream = builder.stream(
                "orders-topic",
                Consumed.with(Serdes.String(), standingOrderSerde)
        );

        stream.mapValues(order -> {
            try {
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

                return processed; // Return the ProcessedStandingOrder object directly
            } catch (Exception e) {
                throw new RuntimeException("Error processing order: " + order, e);
            }
        }).to("processed-orders-topic", Produced.with(Serdes.String(), new ProcessedStandingOrderSerde())); // Use the custom Serde

        return stream;
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
