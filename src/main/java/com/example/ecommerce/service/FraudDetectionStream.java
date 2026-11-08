package com.example.ecommerce.service;

import com.example.ecommerce.config.KafkaTopicAndStreamsConfig;
import com.example.ecommerce.dto.FraudAlertEvent;
import com.example.ecommerce.dto.OrderEventDTO;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
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

    /**
     * KAFKA STREAMS TOPOLOGY (Xử lý luồng thời gian thực):
     * 1. Lắng nghe dòng sự kiện từ topic: order-events (KStream)
     * 2. Gom nhóm theo customerId (groupByKey)
     * 3. Áp dụng Cửa sổ thời gian Tumbling Window 30 giây (windowedBy)
     * 4. Tính tổng số lượng đơn và tổng số tiền của mỗi khách hàng trong 30s đó (aggregate)
     * 5. Nếu >= 3 đơn HOẶC tổng tiền >= 50 triệu -> Bắn ngay cảnh báo vào topic: fraud-alerts!
     */
    @Autowired
    public void buildPipeline(StreamsBuilder streamsBuilder) {
        log.info("🌊 [Kafka Streams] Đang khởi tạo Topology phát hiện gian lận thời gian thực...");

        KStream<String, String> orderStream = streamsBuilder.stream(
                KafkaTopicAndStreamsConfig.TOPIC_ORDERS,
                Consumed.with(Serdes.String(), Serdes.String())
        );

        orderStream
                .groupByKey()
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(30)))
                .aggregate(
                        () -> "0:0.0", // State khởi tạo dạng "count:totalAmount"
                        (customerId, orderJson, aggregate) -> {
                            try {
                                OrderEventDTO order = objectMapper.readValue(orderJson, OrderEventDTO.class);
                                String[] parts = aggregate.split(":");
                                long count = Long.parseLong(parts[0]) + 1;
                                double total = Double.parseDouble(parts[1]) + (order.getAmount() != null ? order.getAmount() : 0.0);
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

                        // Điều kiện kích hoạt Fraud: >= 3 đơn trong 30 giây HOẶC tổng tiền >= 50,000,000 VND
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
                            : "Cảnh báo GIÁ TRỊ LỚN: Tổng đơn hàng vượt ngưỡng 50 triệu (" + String.format("%,.0f", total) + " VND) trong 30 giây!";

                    log.warn("🚨 [Kafka Streams - Fraud Alert] PHÁT HIỆN GIAN LẬN! Khách hàng: {}, Lý do: {}", customerId, reason);

                    FraudAlertEvent alert = new FraudAlertEvent(customerId, count, total, reason);
                    try {
                        String alertJson = objectMapper.writeValueAsString(alert);
                        return KeyValue.pair(customerId, alertJson);
                    } catch (Exception e) {
                        return KeyValue.pair(customerId, "{}");
                    }
                })
                .to(KafkaTopicAndStreamsConfig.TOPIC_FRAUD_ALERTS, Produced.with(Serdes.String(), Serdes.String()));
    }
}
