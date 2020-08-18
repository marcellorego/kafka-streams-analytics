package com.example.demo;

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
public class PageViewEventSource implements ApplicationRunner {

    private final MessageChannel pageViewsOut;

    public PageViewEventSource(AnalyticsBindings analyticsBindings) {
        this.pageViewsOut = analyticsBindings.pageViewsSource();
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {

        ThreadLocalRandom random = ThreadLocalRandom.current();
        List<String> names = Arrays.asList("Marcello", "Miguel", "Tom", "Sergio");
        List<String> pages = Arrays.asList("site", "about", "map", "contact");

        Runnable runnable = () -> {

            String rName = names.get(random.nextInt(names.size()));
            String rPage = pages.get(random.nextInt(pages.size()));

            PageViewEvent pageViewEvent = new PageViewEvent(rName, rPage, Math.round(Math.random() * 1000L));
            Message<PageViewEvent> message = MessageBuilder
                    .withPayload(pageViewEvent)
                    .setHeader(KafkaHeaders.MESSAGE_KEY, pageViewEvent.getUserId().getBytes())
                    .build();
            try {
                pageViewsOut.send(message);
                log.info("message sent {}", message);
            } catch (Exception e) {
                log.error("Error sending message", e);
            }
        };

        Executors.newScheduledThreadPool(1).scheduleAtFixedRate(runnable, 1, 1, TimeUnit.SECONDS);
    }
}