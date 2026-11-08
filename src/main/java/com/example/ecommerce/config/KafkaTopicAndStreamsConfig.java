package com.example.ecommerce.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaTopicAndStreamsConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaTopicAndStreamsConfig.class);

    public static final String TOPIC_ORDERS = "order-events";
    public static final String TOPIC_ORDERS_DLT = "order-events.DLT";
    public static final String TOPIC_FRAUD_ALERTS = "fraud-alerts";
    public static final String TOPIC_PAYMENT_CONFIRMED = "payment-confirmed-events";

    // -------------------------------------------------------------------------
    // 1. TỰ ĐỘNG KHỞI TẠO CÁC TOPIC VỚI SỐ PARTITIONS VÀ REPLICAS CHUẨN
    // -------------------------------------------------------------------------
    @Bean
    public NewTopic orderEventsTopic() {
        return TopicBuilder.name(TOPIC_ORDERS)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic orderEventsDltTopic() {
        return TopicBuilder.name(TOPIC_ORDERS_DLT)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic fraudAlertsTopic() {
        return TopicBuilder.name(TOPIC_FRAUD_ALERTS)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic paymentConfirmedTopic() {
        return TopicBuilder.name(TOPIC_PAYMENT_CONFIRMED)
                .partitions(3)
                .replicas(1)
                .build();
    }

    // -------------------------------------------------------------------------
    // 2. CẤU HÌNH KAFKA STREAMS (Xử lý dòng sự kiện thời gian thực)
    // -------------------------------------------------------------------------
    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kStreamsConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ecommerce-fraud-detector-v1");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        // Giảm thời gian cache để các kết quả windowing được emit ngay lập tức khi test
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        return new KafkaStreamsConfiguration(props);
    }

    // -------------------------------------------------------------------------
    // 3. ERROR HANDLER VỚI DEAD LETTER QUEUE (DLQ / DLT)
    // -------------------------------------------------------------------------
    @Bean
    public ConcurrentKafkaListenerContainerFactory<Object, Object> kafkaListenerContainerFactory(
            ConsumerFactory<Object, Object> consumerFactory,
            KafkaOperations<Object, Object> template) {

        ConcurrentKafkaListenerContainerFactory<Object, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);

        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template);
        FixedBackOff backOff = new FixedBackOff(1000L, 2L); // 2 lần retry, giãn cách 1s

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);
        errorHandler.setRetryListeners((record, ex, deliveryAttempt) -> {
            log.warn("⚠️ [DLQ Retry Listener] Thử lại lần {} do lỗi: {}", deliveryAttempt, ex.getMessage());
        });

        factory.setCommonErrorHandler(errorHandler);
        return factory;
    }
}
