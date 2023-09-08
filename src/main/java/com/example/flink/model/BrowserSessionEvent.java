package com.example.flink.model;


import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Random;

@Data
@AllArgsConstructor
public class BrowserSessionEvent {
    private String userName;
    private String action;
    private long timestamp;



}