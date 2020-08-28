package com.kafka.stream.demo;

import com.kafka.stream.demo.domain.Sale;
import com.kafka.stream.demo.domain.Sales;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.messaging.MessageChannel;

public interface AnalyticsBindings {

    String SALE_IN = "saleIn";
    String SALE_OUT = "saleOut";

    String SALES_COUNT_MV = "salesCountMView";
    String WINDOWED_SALES_COUNT_MV = "windowedSalesCountMView";
    String SUPPRESSED_WINDOWED_SALES_COUNT_MV = "suppressedWindowedSalesCountMView";

    String SALES_COUNT_IN = "salesCountIn";
    String SALES_COUNT_OUT = "salesCountOut";

    @Output(SALE_OUT)
    MessageChannel saleSource();

    @Input(SALE_IN)
    KStream<String, Sale> saleProcess();

    @Output(SALES_COUNT_OUT)
    KStream<String, Sales> salesCountSource();

    @Input(SALES_COUNT_IN)
    KStream<String, Sales> salesCountSink();
}