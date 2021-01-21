package com.kafka.stream.demo;

import com.kafka.stream.demo.domain.Sales;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@RestController
public class QueryStoreController {

	private final Duration windowDuration;
	private final InteractiveQueryService interactiveQueryService;

	public QueryStoreController(@Value("${app.window.duration:0}") long windowDuration,
								InteractiveQueryService interactiveQueryService) {
		this.windowDuration = Duration.of(windowDuration, ChronoUnit.MINUTES);
		this.interactiveQueryService = interactiveQueryService;
	}

	@GetMapping(path = "/counts")
	public List<Sales> listSalesCount() {

		List<Sales> result = new ArrayList<>();

		if (windowDuration.isZero()) {

			ReadOnlyKeyValueStore<String, Long> salesCountStore =
					interactiveQueryService.getQueryableStore(AnalyticsBindings.SALES_COUNT_MV, QueryableStoreTypes.keyValueStore());

			KeyValueIterator<String, Long> keyValueIterator = salesCountStore.all();
			while (keyValueIterator.hasNext()) {
				KeyValue<String, Long> keyValue = keyValueIterator.next();
				result.add(new Sales(keyValue.key, keyValue.value));
			}

		} else {

			ReadOnlyWindowStore<String, Long> salesCountStore =
					interactiveQueryService.getQueryableStore(AnalyticsBindings.WINDOWED_SALES_COUNT_MV, QueryableStoreTypes.windowStore());

			Instant endFilter = Instant.now().minus(windowDuration);
            Instant startFilter = endFilter.minus(windowDuration);

			KeyValueIterator<Windowed<String>, Long> keyValueIterator = salesCountStore.fetchAll(startFilter, endFilter);
			while (keyValueIterator.hasNext()) {
				KeyValue<Windowed<String>, Long> keyValue = keyValueIterator.next();
				Windowed<String> key = keyValue.key;

				Sales sales = new Sales(key.key(), keyValue.value);

				LocalDateTime start = LocalDateTime.ofInstant(key.window().startTime(), ZoneId.systemDefault());
				LocalDateTime end = LocalDateTime.ofInstant(key.window().endTime(), ZoneId.systemDefault());

				sales.setPeriodStart(start);
				sales.setPeriodEnd(end);

				result.add(sales);
			}

		}

		return result;
	}

	@GetMapping(path = "/counts/{product}")
	public Sales listSalesCount(@PathVariable("product") final String product) {

        if (windowDuration.isZero()) {

            ReadOnlyKeyValueStore<String, Long> salesCountStore =
                    interactiveQueryService.getQueryableStore(AnalyticsBindings.SALES_COUNT_MV, QueryableStoreTypes.keyValueStore());

			Long count = salesCountStore.get(product);

            log.info("Found value {}", count);

			return new Sales(product, count);

        } else {

            ReadOnlyWindowStore<String, Long> salesCountStore =
                    interactiveQueryService.getQueryableStore(AnalyticsBindings.WINDOWED_SALES_COUNT_MV, QueryableStoreTypes.windowStore());

			Instant endFilter = Instant.now().minus(windowDuration);
			Instant startFilter = endFilter.minus(windowDuration);

            WindowStoreIterator<Long> longWindowStoreIterator =
					salesCountStore.fetch(product, startFilter, endFilter);

            Long count = longWindowStoreIterator.hasNext() ?
					longWindowStoreIterator.next().value : 0L;

            log.info("Found value {}", count);

			return new Sales(product, count);

        }
    }
}