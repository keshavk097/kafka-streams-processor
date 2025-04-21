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
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

import java.time.LocalDate;

@Component
public class OrderStreamProcessor {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Bean
    public KStream<String, ProcessedStandingOrder> processOrders(StreamsBuilder builder) {
        // Create JsonSerde for both input and output
        JsonSerde<StandingOrder> standingOrderSerde = new JsonSerde<>(StandingOrder.class);
        JsonSerde<ProcessedStandingOrder> processedStandingOrderSerde = new JsonSerde<>(ProcessedStandingOrder.class);

        // Consume the input stream with the StandingOrder
        KStream<String, StandingOrder> stream = builder.stream(
                "orders-topic",
                Consumed.with(Serdes.String(), standingOrderSerde)
        );

        // Apply business logic and map values to ProcessedStandingOrder
        KStream<String, ProcessedStandingOrder> processedStream = stream.mapValues(order -> {
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
        });

        // Produce to the processed-orders-topic with JsonSerde for ProcessedStandingOrder
        processedStream.to("processed-orders-topic", Produced.with(Serdes.String(), processedStandingOrderSerde));

        return processedStream;
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
