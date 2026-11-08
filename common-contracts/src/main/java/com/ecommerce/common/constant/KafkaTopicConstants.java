package com.ecommerce.common.constant;

public final class KafkaTopicConstants {

    private KafkaTopicConstants() {}

    // Topics cho luồng Saga Choreography
    public static final String TOPIC_ORDER_CREATED = "order-created-events";
    public static final String TOPIC_INVENTORY_RESERVED = "inventory-reserved-events";
    public static final String TOPIC_INVENTORY_FAILED = "inventory-failed-events";
    public static final String TOPIC_INVENTORY_RELEASE = "inventory-release-events"; // Lệnh bù trừ (Compensating)
    public static final String TOPIC_PAYMENT_SUCCESS = "payment-success-events";
    public static final String TOPIC_PAYMENT_FAILED = "payment-failed-events";
    public static final String TOPIC_FRAUD_ALERTS = "fraud-alerts";
    public static final String TOPIC_ORDER_DLT = "order-events.DLT";
}
