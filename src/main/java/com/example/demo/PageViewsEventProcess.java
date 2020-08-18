package com.example.demo;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class PageViewsEventProcess {

    @StreamListener
    @SendTo(AnalyticsBindings.PAGE_COUNT_OUT)
    public KStream<String, Long> process(
            @Input(AnalyticsBindings.PAGE_VIEWS_IN) KStream<String, PageViewEvent> eventKStream) {

        KStream<String, Long> windowedLongKTable = eventKStream
                .filter((s, pageViewEvent) -> pageViewEvent.getDuration() > 10)
                .map((KeyValueMapper<String, PageViewEvent, KeyValue<String, Long>>) (s, pageViewEvent) -> KeyValue.pair(pageViewEvent.getPage(), 0L))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
                //.groupByKey()
                //.windowedBy(TimeWindows.of(Duration.of(10, ChronoUnit.SECONDS)))
                .count(Materialized.as(AnalyticsBindings.PAGE_COUNT_MV))
                .toStream();

        //.mapValues((windowedKey, value) -> new PageViewEventCount(windowedKey.key(), value, windowedKey.window().startTime(), windowedKey.window().endTime()))
        //.toStream();

        //.map((key, value) -> new KeyValue<>(key.key(), new PageViewEventCount(key.key(), value, key.window().startTime(), key.window().endTime())));
        //.toStream((windowedKey, value) -> KeyValue.pair(windowedKey.key(), new PageViewEventCount(windowedKey.key(), value, windowedKey.window().startTime(), windowedKey.window().endTime())));

//            log.debug("process stream {}", eventKStream.keySerde());

        return windowedLongKTable;
    }
}