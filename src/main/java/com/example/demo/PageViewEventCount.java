package com.example.demo;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@NoArgsConstructor
public class PageViewEventCount {
    private String page;
    private long count;
    private Instant start;
    private Instant end;

    public PageViewEventCount(String page, long count) {
        this.page = page;
        this.count = count;
    }
}