package com.kafka.stream.demo;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.kafka.stream.demo.domain.Sales;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.context.annotation.Bean;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;
import org.springframework.kafka.support.serializer.JsonSerde;

@SpringBootApplication
@EnableBinding(AnalyticsBindings.class)
public class DemoApplication {

    @Bean
    public ObjectMapper objectMapper() {

        Jackson2ObjectMapperBuilder builder = new Jackson2ObjectMapperBuilder();

        ObjectMapper objectMapper = builder.build();
        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

        return objectMapper;
    }

    @Bean
    public KeyValueStore<String, Long> countStore() {
        return Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(AnalyticsBindings.SALES_COUNT_MV), Serdes.String(),
                Serdes.Long())
                .build();
    }

    @Bean
    public JsonSerde<Sales> viewEventCountJsonSerde(ObjectMapper objectMapper) {
        return new JsonSerde<>(objectMapper);
    }

    public static void main(String[] args) {
        SpringApplication.run(DemoApplication.class, args);
    }
}




