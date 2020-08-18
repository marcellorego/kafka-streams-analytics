package com.example.demo;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@RestController
public class QueryStoreController {

	private final InteractiveQueryService interactiveQueryService;

	public QueryStoreController(InteractiveQueryService interactiveQueryService) {
		this.interactiveQueryService = interactiveQueryService;
	}

	@GetMapping(path = "/counts")
	public List<PageViewEventCount> listPageCount() {

		final ReadOnlyKeyValueStore<String, Long> pageCountStore =
				this.interactiveQueryService.getQueryableStore(AnalyticsBindings.PAGE_COUNT_MV, QueryableStoreTypes.keyValueStore());

//		ReadOnlyKeyValueStore<String, PageViewEventCount> keyValueStore =
//				streamsBuilderFactoryBean.getKafkaStreams()
//						.store(StoreQueryParameters.fromNameAndType(AnalyticsBindings.PAGE_COUNT_MV, QueryableStoreTypes.keyValueStore()));

		KeyValueIterator<String, Long> keyValueIterator = pageCountStore.all();
		List<PageViewEventCount> result = new ArrayList<>();
		while (keyValueIterator.hasNext()) {
			KeyValue<String, Long> keyValue = keyValueIterator.next();
			result.add(new PageViewEventCount(keyValue.key, keyValue.value));
		}
		return result;
	}

	@GetMapping(path = "/counts/{name}")
	public PageViewEventCount listPageCount(@PathVariable("name") final String name) {

		final ReadOnlyKeyValueStore<String, PageViewEventCount> pageCountStore =
				interactiveQueryService.getQueryableStore(AnalyticsBindings.PAGE_COUNT_MV, QueryableStoreTypes.keyValueStore());


//		ReadOnlyKeyValueStore<String, PageViewEventCount> keyValueStore =
//				streamsBuilderFactoryBean.getKafkaStreams()
//						.store(StoreQueryParameters.fromNameAndType(AnalyticsBindings.PAGE_COUNT_MV, QueryableStoreTypes.keyValueStore()));

		final PageViewEventCount pageViewEventCount = pageCountStore.get(name);

		log.info("Found value {}", pageViewEventCount);

		return pageViewEventCount;
	}
}