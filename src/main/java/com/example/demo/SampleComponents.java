package com.example.demo;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.context.annotation.Bean;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class SampleComponents {

//    private static <T> JsonSerde<T> jsonSerdeBuilder(ObjectMapper mapper, Class<T> targetClass) {
//
//        JsonSerializer<T> serializer = new JsonSerializer<>(mapper);
//        JsonDeserializer<T> deserializer = new JsonDeserializer<>(targetClass, mapper, false);
//        deserializer.addTrustedPackages("com.example.demo");
//
//        return new JsonSerde<T>(
//                serializer,
//                deserializer);
//    }
//
//    private static <K, V> Materialized<K, V, KeyValueStore<Bytes, byte[]>> materializedAsPersistentStore(
//            String storeName,
//            Serde<K> keySerde,
//            Serde<V> valueSerde
//    ) {
//        return Materialized.<K, V>as(Stores.persistentKeyValueStore(storeName))
//                .withKeySerde(keySerde)
//                .withValueSerde(valueSerde);
//    }
//
//    private static <K, V> Materialized<K, V, WindowStore<Bytes, byte[]>> materializedAsWindowStore(
//            String storeName,
//            Serde<K> keySerde,
//            Serde<V> valueSerde,
//            Duration windowDuration
//    ) {
//        return Materialized.<K, V>as(Stores.persistentWindowStore(storeName, windowDuration, windowDuration, false))
//                .withKeySerde(keySerde)
//                .withValueSerde(valueSerde);
//    }
//
//    @Bean
//    public ObjectMapper objectMapper() {
//
//        Jackson2ObjectMapperBuilder builder = new Jackson2ObjectMapperBuilder();
//
//        ObjectMapper objectMapper = builder.build();
//        objectMapper.registerModule(new JavaTimeModule());
//        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
//        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
//        objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
//
//        return objectMapper;
//    }
//
//    @Bean
//    public JsonSerde<PageViewEventCount> viewEventCountJsonSerde(ObjectMapper objectMapper) {
//        return jsonSerdeBuilder(objectMapper, PageViewEventCount.class);
//    }
//
//    @Bean
//    public KeyValueStore<String, Long> countStore() {
//        return Stores.keyValueStoreBuilder(
//                Stores.persistentKeyValueStore(AnalyticsBindings.PAGE_COUNT_MV),
//                Serdes.String(),
//                Serdes.Long())
//                .build();
//    }
//
//    @Component
//    @Slf4j
//    public static final class PageViewEventSource implements ApplicationRunner {
//
//        private final MessageChannel pageViewsOut;
//
//        public PageViewEventSource(AnalyticsBindings analyticsBindings) {
//            this.pageViewsOut = analyticsBindings.pageViewsSource();
//        }
//
//        @Override
//        public void run(ApplicationArguments args) throws Exception {
//
//            List<String> names = Arrays.asList("Marcello", "Miguel", "Tom", "Sergio");
//            List<String> apps = Arrays.asList("CINT", "UMS", "CT", "SB");
//
//            Runnable runnable = () -> {
//
//                String rName = names.get(ThreadLocalRandom.current().nextInt(names.size()));
//                String rPage = apps.get(ThreadLocalRandom.current().nextInt(apps.size()));
//
//                PageViewEvent pageViewEvent = new PageViewEvent(rName, rPage, Math.round(Math.random() * 1000L));
//                Message<PageViewEvent> message = MessageBuilder
//                        .withPayload(pageViewEvent)
//                        .setHeader(KafkaHeaders.MESSAGE_KEY, pageViewEvent.getUserId().getBytes())
//                        .build();
//                try {
//                    pageViewsOut.send(message);
//                    log.info("message sent {}", message);
//                } catch (Exception e) {
//                    log.error("Error sending message", e);
//                }
//            };
//
//            Executors.newScheduledThreadPool(1).scheduleAtFixedRate(runnable, 1, 1, TimeUnit.SECONDS);
//        }
//    }
//
//    @Component
//    @Slf4j
//    public static class PageViewsEventSink {
//
//        private final JsonSerde<PageViewEventCount> viewEventCountJsonSerde;
//
//        public PageViewsEventSink(JsonSerde<PageViewEventCount> viewEventCountJsonSerde) {
//            this.viewEventCountJsonSerde = viewEventCountJsonSerde;
//        }
//
////        @StreamListener
////        @SendTo(AnalyticsBindings.PAGE_COUNT_OUT)
////        public KStream<String, PageViewEventCount> process(
////                @Input(AnalyticsBindings.PAGE_VIEWS_IN) KStream<String, PageViewEvent> eventKStream) {
////
////            KStream<String, PageViewEventCount> windowedLongKTable = eventKStream
////                    //.filter((s, pageViewEvent) -> pageViewEvent.getDuration() > 10)
////                    .map((KeyValueMapper<String, PageViewEvent, KeyValue<String, PageViewEventCount>>) (s, pageViewEvent) ->
////                            KeyValue.pair(pageViewEvent.getPage(), new PageViewEventCount(pageViewEvent.getPage(), 0L)))
////                    .groupByKey(Grouped.with(AnalyticsBindings.PAGE_COUNT_GP, Serdes.String(), viewEventCountJsonSerde))
////                    //.groupByKey(Grouped.with(Serdes.String(), viewEventCountJsonSerde))
////                    //.groupByKey()
////                    //.windowedBy(TimeWindows.of(Duration.of(10, ChronoUnit.SECONDS)))
////                    //.count(Materialized.as(AnalyticsBindings.PAGE_COUNT_MV))
////                    .aggregate(new Initializer<PageViewEventCount>() {
////                        @Override
////                        public PageViewEventCount apply() {
////                            return null;
////                        }
////                    }, new Aggregator<String, PageViewEventCount, PageViewEventCount>() {
////                        @Override
////                        public PageViewEventCount apply(String s, PageViewEventCount pageViewEventCount1, PageViewEventCount pageViewEventCount2) {
////                            return new PageViewEventCount( s, pageViewEventCount1.getCount() + (pageViewEventCount2 != null ? pageViewEventCount2.getCount() : 0L));
////                        }
////                    })
////                    .toStream();
////
////            //.mapValues((windowedKey, value) -> new PageViewEventCount(windowedKey.key(), value, windowedKey.window().startTime(), windowedKey.window().endTime()))
////            //.toStream();
////
////            //.map((key, value) -> new KeyValue<>(key.key(), new PageViewEventCount(key.key(), value, key.window().startTime(), key.window().endTime())));
////            //.toStream((windowedKey, value) -> KeyValue.pair(windowedKey.key(), new PageViewEventCount(windowedKey.key(), value, windowedKey.window().startTime(), windowedKey.window().endTime())));
////
//////            log.debug("process stream {}", eventKStream.keySerde());
////
////            return windowedLongKTable;
////        }
//
//        @StreamListener
//        @SendTo(AnalyticsBindings.PAGE_COUNT_OUT)
//        public KStream<String, Long> process(
//                @Input(AnalyticsBindings.PAGE_VIEWS_IN) KStream<String, PageViewEvent> eventKStream) {
//
//            KStream<String, Long> windowedLongKTable = eventKStream
//                    //.filter((s, pageViewEvent) -> pageViewEvent.getDuration() > 10)
//                    .map((KeyValueMapper<String, PageViewEvent, KeyValue<String, Long>>) (s, pageViewEvent) -> KeyValue.pair(pageViewEvent.getPage(), 0L))
//                    .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
//                    //.groupByKey()
//                    //.windowedBy(TimeWindows.of(Duration.of(10, ChronoUnit.SECONDS)))
//                    .count(Materialized.as(AnalyticsBindings.PAGE_COUNT_MV))
//                    .toStream();
//
//            //.mapValues((windowedKey, value) -> new PageViewEventCount(windowedKey.key(), value, windowedKey.window().startTime(), windowedKey.window().endTime()))
//            //.toStream();
//
//            //.map((key, value) -> new KeyValue<>(key.key(), new PageViewEventCount(key.key(), value, key.window().startTime(), key.window().endTime())));
//            //.toStream((windowedKey, value) -> KeyValue.pair(windowedKey.key(), new PageViewEventCount(windowedKey.key(), value, windowedKey.window().startTime(), windowedKey.window().endTime())));
//
////            log.debug("process stream {}", eventKStream.keySerde());
//
//            return windowedLongKTable;
//        }
//
//
////        @StreamListener
////        @SendTo(AnalyticsBindings.PAGE_COUNT_OUT)
////        public KStream<String, PageViewEventCount> process(
////                @Input(AnalyticsBindings.PAGE_VIEWS_IN) KStream<String, PageViewEvent> eventKStream) {
////
////            Materialized<String, PageViewEventCount, KeyValueStore<Bytes, byte[]>> materialized =
////                    materializedAsPersistentStore(AnalyticsBindings.PAGE_COUNT_MV, Serdes.String(), viewEventCountJsonSerde);
////
////            KGroupedStream<String, PageViewEventCount> pagesByName = eventKStream
////                    .filter((s, pageViewEvent) -> pageViewEvent.getDuration() > 10)
////                    .map((KeyValueMapper<String, PageViewEvent, KeyValue<String, PageViewEventCount>>) (s, pageViewEvent) ->
////                            KeyValue.pair(pageViewEvent.getPage(), new PageViewEventCount(pageViewEvent.getPage(), 0L)))
////                    .groupByKey(Grouped.with(Serdes.String(), viewEventCountJsonSerde));
////
////            KStream<String, PageViewEventCount> aggregated =
////                    pagesByName
////                            .aggregate(
////                                    () -> new PageViewEventCount(null, 0L),
////
////                                    (s, pageViewEventCount, pageViewEventCurrent) ->
////                                            new PageViewEventCount(s, pageViewEventCount.getCount() +
////                                                    Optional.ofNullable(pageViewEventCurrent)
////                                                            .map(PageViewEventCount::getCount)
////                                                            .orElse(0L)),
////
////                                    materialized
////                            )
////                            .toStream();
////
////            return aggregated;
////        }
//
//    }
//
//    @Component
//    @Slf4j
//    public static class PageCountSink {
//
//        @StreamListener
//        public void process(@Input(AnalyticsBindings.PAGE_COUNT_IN) KStream<String, Long> counts) {
//            counts.foreach((key, value) -> log.info(key + "=" + value));
//        }
//    }
//
//    public static void main(String[] args) {
//        SpringApplication.run(Demo2Application.class, args);
//    }
}




