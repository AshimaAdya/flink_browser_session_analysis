package com.example.flink.model;

import org.apache.flink.api.common.functions.MapFunction;

public class BrowserSessionEventDeserializer implements MapFunction<String, BrowserSessionEvent> {
    @Override
    public BrowserSessionEvent map(String value) throws Exception {
        System.out.println("--- Received Record : " + value);

        String[] parts = value.split(",");

        if (parts.length != 3) {
            // Handle invalid input data gracefully
            return null;
        }

        String userName = parts[0].trim();
        String action = parts[1].trim();
        long timestamp = Long.parseLong(parts[2].trim());

        return new BrowserSessionEvent(userName, action, timestamp);
    }
}

