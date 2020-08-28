package com.kafka.stream.demo;

import com.kafka.stream.demo.domain.Sales;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class SalesCountSink {

    @StreamListener
    public void process(@Input(AnalyticsBindings.SALES_COUNT_IN) KStream<String, Sales> counts) {
        counts.foreach((key, value) -> log.info(key + "=" + value));
    }
}