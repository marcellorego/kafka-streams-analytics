package com.example.demo;

import lombok.*;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PageViewEvent {
    private String userId;
    private String page;
    private long duration;
}