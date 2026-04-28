package com.example.orders.model;

import java.util.List;

public record Order(
        String orderId,
        String customerId,
        double amount,
        List<String> items
) {}
