package com.kafka.stream.demo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.stream.demo.domain.Sale;
import com.kafka.stream.demo.domain.Sales;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;

@Component
@Slf4j
public class SalesEventProcess {

    private final ObjectMapper mapper;
    private final Duration windowDuration;
    private final Duration retentionPeriod;
    private final Serde<String> stringSerde;
    private final Serde<Long> longSerde;
    private final Serde<Sale> saleSerde;
    private final Serde<Sales> salesSerde;

    public SalesEventProcess(ObjectMapper mapper,
                             @Value("${app.window.duration:0}") long windowDuration,
                             @Value("${app.window.retentionPeriod:0}") long retentionPeriod) {
        this.mapper = mapper;
        this.windowDuration = Duration.of(windowDuration, ChronoUnit.MINUTES);
        this.retentionPeriod = Duration.of(retentionPeriod, ChronoUnit.MINUTES);
        this.stringSerde = Serdes.String();
        this.longSerde = Serdes.Long();
        this.saleSerde = jsonSerde(Sale.class);
        this.salesSerde = jsonSerde(Sales.class);
    }

    @StreamListener
    @SendTo(AnalyticsBindings.SALES_COUNT_OUT)
    public KStream<String, Sales> processSales(
            @Input(AnalyticsBindings.SALE_IN) KStream<String, Sale> saleKStream) {

        KGroupedStream<String, Sale> saleByProduct = saleKStream
                .filter((key, sale) -> sale.getAmount() > 0)
                .groupByKey();

        // If no window in configured then we perform a regular aggregation
        // We just store the aggregated amount in the state store
        // When we aggregate this way intermediate results will be sent to Kafka regularly
        if (windowDuration.isZero()) {

            return saleByProduct
                    .aggregate(this::initialize,
                            this::aggregateAmount,
                            materializedAsPersistentStore(AnalyticsBindings.SALES_COUNT_MV, stringSerde, longSerde))
                    .toStream()
                    .mapValues(Sales::new);
        }

        // Now if we have a window configured we aggregate sales contained in a time window
        // - First we need to window the incoming records. We use a time window which is a fixed-size window.
        // - Then we aggregate the records. Here we use a different materialized that relies on a WindowStore
        // rather than a regular KeyValueStore.
        // - Here we only want the final aggregation of each period to be send to Kakfa. To do
        // that we use the suppress method and tell it to suppress all records until the window closes.
        // - Then we just need to map the key before sending to Kafka because the windowing operation changed
        // it into a windowed key. We also inject the window start and end timestamps into the final record.
        return saleByProduct
                .windowedBy(TimeWindows.of(windowDuration).grace(Duration.ZERO))
                .aggregate(this::initialize, this::aggregateAmount,
                        materializedAsWindowStore(AnalyticsBindings.WINDOWED_SALES_COUNT_MV, stringSerde, longSerde))
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded())
                        .withName(AnalyticsBindings.SUPPRESSED_WINDOWED_SALES_COUNT_MV))
                .toStream()
                .map((key, aggregatedAmount) -> {
                    LocalDateTime start = LocalDateTime.ofInstant(key.window().startTime(), ZoneId.systemDefault());
                    LocalDateTime end = LocalDateTime.ofInstant(key.window().endTime(), ZoneId.systemDefault());
                    return KeyValue.pair(key.key(), new Sales(key.key(), aggregatedAmount, start, end));
                });

//        KStream<String, Long> countStream = saleByProduct
//                //.groupByKey()
//                //.windowedBy(TimeWindows.of(Duration.of(10, ChronoUnit.SECONDS)))
//                .count(Materialized.as(AnalyticsBindings.SALES_COUNT_MV))
//                .toStream();

        //.mapValues((windowedKey, value) -> new PageViewEventCount(windowedKey.key(), value, windowedKey.window().startTime(), windowedKey.window().endTime()))
        //.toStream();

        //.map((key, value) -> new KeyValue<>(key.key(), new PageViewEventCount(key.key(), value, key.window().startTime(), key.window().endTime())));
        //.toStream((windowedKey, value) -> KeyValue.pair(windowedKey.key(), new PageViewEventCount(windowedKey.key(), value, windowedKey.window().startTime(), windowedKey.window().endTime())));

//            log.debug("process stream {}", eventKStream.keySerde());
    }

    private Long initialize() {
        return 0L;
    }

    private Long aggregateAmount(String key, Sale sale, Long aggregatedAmount) {
        return aggregatedAmount + sale.getAmount();
    }

    private <T> Serde<T> jsonSerde(Class<T> targetClass) {

        JsonSerializer<T> serializer = new JsonSerializer<>(mapper);
        JsonDeserializer<T> deserializer = new JsonDeserializer<>(targetClass, mapper, false);
        deserializer.addTrustedPackages("com.kafka.stream.demo.domain");

        return Serdes.serdeFrom(
                serializer,
                deserializer
        );
    }

    private <K, V> Materialized<K, V, KeyValueStore<Bytes, byte[]>> materializedAsPersistentStore(
            String storeName,
            Serde<K> keySerde,
            Serde<V> valueSerde
    ) {
        return Materialized.<K, V>as(Stores.persistentKeyValueStore(storeName))
                .withKeySerde(keySerde)
                .withValueSerde(valueSerde);
    }

    private <K, V> Materialized<K, V, WindowStore<Bytes, byte[]>> materializedAsWindowStore(
            String storeName,
            Serde<K> keySerde,
            Serde<V> valueSerde
    ) {
        return Materialized.<K, V>as(Stores.persistentWindowStore(storeName, retentionPeriod, windowDuration, false))
                .withKeySerde(keySerde)
                .withValueSerde(valueSerde);
    }
}