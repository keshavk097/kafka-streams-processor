package com.example.kafka_streams_processor.model;

import com.example.kafka_streams_processor.serdes.LocalDateDeserializer;
import com.example.kafka_streams_processor.serdes.LocalDateSerializer;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;


import lombok.Data;

import java.time.LocalDate;

@Data
public class ProcessedStandingOrder {

    private String orderId;

    private String customerId;

    private String name;

    private String accountNumber;

    private String sortCode;

    private Double amount;

    private String frequency;

    private Double accountBalance;

    @JsonFormat(pattern = "yyyy-MM-dd") // Optional: Specify the format for LocalDate
    @JsonSerialize(using = LocalDateSerializer.class) // Use a custom serializer (optional)
    @JsonDeserialize(using = LocalDateDeserializer.class)
    private LocalDate nextExecutionDate;

    private String status;

    @JsonFormat(pattern = "yyyy-MM-dd") // Optional: Specify the format for LocalDate
    @JsonSerialize(using = LocalDateSerializer.class) // Use a custom serializer (optional)
    @JsonDeserialize(using = LocalDateDeserializer.class)
    private LocalDate startDate;

    @JsonFormat(pattern = "yyyy-MM-dd") // Optional: Specify the format for LocalDate
    @JsonSerialize(using = LocalDateSerializer.class) // Use a custom serializer (optional)
    @JsonDeserialize(using = LocalDateDeserializer.class)
    private LocalDate endDate;
}
