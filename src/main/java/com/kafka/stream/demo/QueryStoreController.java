package com.kafka.stream.demo;

import com.kafka.stream.demo.domain.Sales;
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
	public List<Sales> listSalesCount() {

		ReadOnlyKeyValueStore<String, Long> salesCountStore =
				interactiveQueryService.getQueryableStore(AnalyticsBindings.SALES_COUNT_MV, QueryableStoreTypes.keyValueStore());

		KeyValueIterator<String, Long> keyValueIterator = salesCountStore.all();
		List<Sales> result = new ArrayList<>();

		while (keyValueIterator.hasNext()) {
			KeyValue<String, Long> keyValue = keyValueIterator.next();
			Sales salesCount = new Sales(keyValue.key, keyValue.value);
			result.add(salesCount);
		}

		return result;
	}

	@GetMapping(path = "/counts/{product}")
	public Sales listSalesCount(@PathVariable("product") final String product) {

		ReadOnlyKeyValueStore<String, Long> salesCountStore =
				interactiveQueryService.getQueryableStore(AnalyticsBindings.SALES_COUNT_MV, QueryableStoreTypes.keyValueStore());

		final Long count = salesCountStore.get(product);

		log.info("Found value {}", count);

		return new Sales(product, count);
	}
}