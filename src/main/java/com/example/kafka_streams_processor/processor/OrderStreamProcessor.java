package com.example.kafka_streams_processor.processor;



import com.example.kafka_streams_processor.model.ProcessedStandingOrder;
import com.example.kafka_streams_processor.model.StandingOrder;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.time.LocalDate;

@Component
public class OrderStreamProcessor {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Bean
    public KStream<String, String> processOrders(StreamsBuilder builder) {
        KStream<String, String> stream = builder.stream("orders-topic");

        stream.mapValues(orderJson -> {
            try {
                StandingOrder order = objectMapper.readValue(orderJson, StandingOrder.class);
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
                return objectMapper.writeValueAsString(processed);
            } catch (Exception e) {
                throw new RuntimeException("Error processing order: " + orderJson, e);
            }
        }).to("processed-orders-topic");

        return stream;
    }
    private String getStatus(StandingOrder standingOrder){
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

    private LocalDate getNextExecutionDate(StandingOrder standingOrder){
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

