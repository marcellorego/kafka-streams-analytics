package com.example.demo;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class PageCountSink {

    @StreamListener
    public void process(@Input(AnalyticsBindings.PAGE_COUNT_IN) KStream<String, Long> counts) {
        counts.foreach((key, value) -> log.info(key + "=" + value));
    }
}