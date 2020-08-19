package com.kafka.stream.demo;

import com.kafka.stream.demo.domain.Sale;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
public class SaleEventSource implements ApplicationRunner {

    private final MessageChannel saleSourceOut;

    public SaleEventSource(AnalyticsBindings analyticsBindings) {
        this.saleSourceOut = analyticsBindings.saleSource();
    }

    @Override
    public void run(ApplicationArguments args) {

        ThreadLocalRandom random = ThreadLocalRandom.current();
        List<String> names = Arrays.asList("Marcello", "Miguel", "Tom", "Sergio");
        List<String> products = Arrays.asList("car", "house", "yacht", "plane");

        Runnable runnable = () -> {

            String rName = names.get(random.nextInt(names.size()));
            String rProduct = products.get(random.nextInt(products.size()));

            Sale sale = new Sale(rName, rProduct, (long) random.nextInt(1, 5));
            Message<Sale> message = MessageBuilder
                    .withPayload(sale)
                    .setHeader(KafkaHeaders.MESSAGE_KEY, sale.getUserId().getBytes())
                    .build();
            try {
                saleSourceOut.send(message);
                //log.info("message sent {}", message);
            } catch (Exception e) {
                log.error("Error sending message", e);
            }
        };

        Executors.newScheduledThreadPool(1).scheduleAtFixedRate(runnable, 1, 1, TimeUnit.SECONDS);
    }
}