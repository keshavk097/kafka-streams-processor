package com.example.kafka_streams_processor.processor;

import com.example.kafka_streams_processor.model.CustomerAccount;
import com.example.kafka_streams_processor.model.ProcessedStandingOrder;
import com.example.kafka_streams_processor.model.StandingOrder;
import com.example.kafka_streams_processor.serdes.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.LocalDate;
import java.util.Map;

@Component
public class StreamsProcessor {
    private static final Logger logger = LoggerFactory.getLogger(StreamsProcessor.class);

    @Bean
    public org.apache.kafka.streams.KafkaStreams  joinStreams() {

        StreamsBuilder builder = new StreamsBuilder();

        var orderSerde = Serdes.serdeFrom(new OrderSerializer(), new OrderDeserializer());
        var enhancedSerde = Serdes.serdeFrom(new EnhancedOrderSerializer(), new EnhancedOrderDeserializer());
        var customerSerde = Serdes.serdeFrom(new CustomerAccountSerializer(), new CustomerAccountDeserializer());

        logger.debug("Setting up Kafka Streams topology for order and customer join");

        KStream<String, StandingOrder> standingOrderStream = builder
                .stream("orders-topic", Consumed.with(Serdes.String(), orderSerde))
                .peek((key, value) -> logger.debug("Received StandingOrder: key={}, value={}", key, value));

        KStream<String, StandingOrder> rekeyedStandingOrderStream = standingOrderStream
                .selectKey((key, value) -> {
                        logger.debug("Rekeying order with customerId: {}", value.getCustomerId());
                        return value.getCustomerId();
                });
        KStream<String, CustomerAccount> customerAccountStream = builder
                .stream("customer-account-detail-topic", Consumed.with(Serdes.String(), customerSerde))
                .peek((key, value) -> logger.debug("Received CustomerAccount: key={}, value={}", key, value));

        KStream<String, ProcessedStandingOrder> joinedStream = rekeyedStandingOrderStream.join(
                customerAccountStream,
                (order, account) -> {
                    logger.debug("Joining order {} with account {}", order.getOrderId(), account.getName());
                    ProcessedStandingOrder processedOrder = new ProcessedStandingOrder();
                    processedOrder.setOrderId(order.getOrderId());
                    processedOrder.setCustomerId(order.getCustomerId());
                    processedOrder.setAmount(order.getAmount());
                    processedOrder.setAccountNumber(order.getAccountNumber());
                    processedOrder.setSortCode(order.getSortCode());
                    processedOrder.setStartDate(order.getStartDate());
                    processedOrder.setEndDate(order.getEndDate());
                    processedOrder.setFrequency(order.getFrequency());
                    processedOrder.setStatus(getStatus(order));
                    processedOrder.setNextExecutionDate(getNextExecutionDate(order));
                    processedOrder.setName(account.getName());
                    processedOrder.setAccountBalance(account.getAccountBalance());
                    return processedOrder;
                },
                JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(5)),
                StreamJoined.with(Serdes.String(), orderSerde, customerSerde)
        );
        // Output to topic
        joinedStream.to("standing-order-detail-topic", Produced.with(Serdes.String(), enhancedSerde));
        logger.info("Topology defined, writing joined records to standing-order-detail-topic");

        var props = Map.of(
                StreamsConfig.APPLICATION_ID_CONFIG, "order-stream-processor",
                StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "my-kafka-kafka-bootstrap.kafka.svc:9092"
        );

        var streams = new org.apache.kafka.streams.KafkaStreams(builder.build(), new StreamsConfig(props));
        logger.info("Starting Kafka Streams application");
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
