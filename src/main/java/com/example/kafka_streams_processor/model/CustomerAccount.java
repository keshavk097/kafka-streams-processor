package com.example.kafka_streams_processor.model;

import lombok.Data;

@Data
public class CustomerAccount {
    private String customerId;
    private String accountNumber;
    private String name;
    private Double accountBalance;
}
