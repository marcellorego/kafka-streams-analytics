package com.example.demo;

import org.apache.kafka.streams.kstream.KStream;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.messaging.MessageChannel;

public interface AnalyticsBindings {

    String PAGE_VIEWS_IN = "pvin";
    String PAGE_VIEWS_OUT = "pvout";

    String PAGE_COUNT_MV = "pcmv";

    String PAGE_COUNT_IN = "pcin";
    String PAGE_COUNT_OUT = "pcout";

    @Output(PAGE_VIEWS_OUT)
    MessageChannel pageViewsSource();

    @Input(PAGE_VIEWS_IN)
    KStream<String, PageViewEvent> pageViewProcess();

    @Output(PAGE_COUNT_OUT)
    KStream<String, Long> pageCountSource();

    @Input(PAGE_COUNT_IN)
    KStream<String, Long> pageCountSink();
}