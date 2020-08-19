package com.kafka.stream.demo.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Sales {
	private String productId;
	private Long amount;
	private LocalDateTime periodStart;
	private LocalDateTime periodEnd;

	public Sales(String productId, Long amount) {
		this(productId, amount, null, null);
	}
}
