package com.ecommerce.fraud.service;

import com.ecommerce.common.constant.KafkaTopicConstants;
import com.ecommerce.common.event.FraudAlertEvent;
import com.ecommerce.common.event.OrderCreatedEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
public class FraudDetectionStream {

    private static final Logger log = LoggerFactory.getLogger(FraudDetectionStream.class);

    private final ObjectMapper objectMapper;

    public FraudDetectionStream(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Autowired
    public void buildPipeline(StreamsBuilder streamsBuilder) {
        log.info("🌊 [Kafka Streams Engine] Khởi tạo Topology phân tích gian lận thời gian thực (Cửa sổ 30s)...");

        KStream<String, String> orderStream = streamsBuilder.stream(
                KafkaTopicConstants.TOPIC_ORDER_CREATED,
                Consumed.with(Serdes.String(), Serdes.String())
        );

        orderStream
                .groupByKey()
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(30)))
                .aggregate(
                        () -> "0:0.0", // Dạng count:totalAmount
                        (customerId, orderJson, aggregate) -> {
                            try {
                                OrderCreatedEvent order = objectMapper.readValue(orderJson, OrderCreatedEvent.class);
                                String[] parts = aggregate.split(":");
                                long count = Long.parseLong(parts[0]) + 1;
                                double total = Double.parseDouble(parts[1]) + (order.getTotalAmount() != null ? order.getTotalAmount() : 0.0);
                                return count + ":" + total;
                            } catch (Exception e) {
                                return aggregate;
                            }
                        },
                        Materialized.with(Serdes.String(), Serdes.String())
                )
                .toStream()
                .filter((windowedKey, aggregate) -> {
                    try {
                        String[] parts = aggregate.split(":");
                        long count = Long.parseLong(parts[0]);
                        double total = Double.parseDouble(parts[1]);

                        // Điều kiện: >= 3 đơn trong 30s HOẶC tổng tiền >= 50,000,000 VND
                        return count >= 3 || total >= 50_000_000.0;
                    } catch (Exception e) {
                        return false;
                    }
                })
                .map((windowedKey, aggregate) -> {
                    String[] parts = aggregate.split(":");
                    long count = Long.parseLong(parts[0]);
                    double total = Double.parseDouble(parts[1]);
                    String customerId = windowedKey.key();

                    String reason = count >= 3
                            ? "Cảnh báo SPAM: Khách hàng đặt " + count + " đơn liên tục trong vòng 30 giây!"
                            : "Cảnh báo GIÁ TRỊ LỚN: Tổng đơn hàng đạt " + String.format("%,.0f", total) + " VND trong vòng 30 giây!";

                    log.warn("🚨 [Kafka Streams Alert] Phát hiện hành vi bất thường của khách: {}. {}", customerId, reason);

                    FraudAlertEvent alert = new FraudAlertEvent(customerId, count, total, reason);
                    try {
                        String json = objectMapper.writeValueAsString(alert);
                        return KeyValue.pair(customerId, json);
                    } catch (Exception e) {
                        return KeyValue.pair(customerId, "{}");
                    }
                })
                .to(KafkaTopicConstants.TOPIC_FRAUD_ALERTS, Produced.with(Serdes.String(), Serdes.String()));
    }
}

