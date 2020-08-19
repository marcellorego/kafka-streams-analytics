package com.kafka.stream.demo;

import com.kafka.stream.demo.domain.Sale;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class SalesEventProcess {

    @StreamListener
    @SendTo(AnalyticsBindings.SALES_COUNT_OUT)
    public KStream<String, Long> process(
            @Input(AnalyticsBindings.SALE_IN) KStream<String, Sale> saleKStream) {

        KGroupedStream<String, Long> countProducts = saleKStream
                .filter((key, sale) -> sale.getAmount() > 0)
                .map((KeyValueMapper<String, Sale, KeyValue<String, Long>>) (key, sale) -> KeyValue.pair(sale.getProductId(), 0L))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()));

        KStream<String, Long> countStream = countProducts
                //.groupByKey()
                //.windowedBy(TimeWindows.of(Duration.of(10, ChronoUnit.SECONDS)))
                .count(Materialized.as(AnalyticsBindings.SALES_COUNT_MV))
                .toStream();

        //.mapValues((windowedKey, value) -> new PageViewEventCount(windowedKey.key(), value, windowedKey.window().startTime(), windowedKey.window().endTime()))
        //.toStream();

        //.map((key, value) -> new KeyValue<>(key.key(), new PageViewEventCount(key.key(), value, key.window().startTime(), key.window().endTime())));
        //.toStream((windowedKey, value) -> KeyValue.pair(windowedKey.key(), new PageViewEventCount(windowedKey.key(), value, windowedKey.window().startTime(), windowedKey.window().endTime())));

//            log.debug("process stream {}", eventKStream.keySerde());

        return countStream;
    }
}