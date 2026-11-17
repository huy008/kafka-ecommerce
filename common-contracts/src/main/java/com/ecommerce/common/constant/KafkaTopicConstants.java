package com.ecommerce.common.constant;

public final class KafkaTopicConstants {

    private KafkaTopicConstants() {}

    // Topics for Saga Choreography workflow
    public static final String TOPIC_ORDER_CREATED = "order-created-events";
    public static final String TOPIC_INVENTORY_RESERVED = "inventory-reserved-events";
    public static final String TOPIC_INVENTORY_FAILED = "inventory-failed-events";
    public static final String TOPIC_INVENTORY_RELEASE = "inventory-release-events"; // Compensating transaction (Release stock)
    public static final String TOPIC_PAYMENT_SUCCESS = "payment-success-events";
    public static final String TOPIC_PAYMENT_FAILED = "payment-failed-events";
    public static final String TOPIC_VOUCHER_APPLIED = "voucher-applied-events";
    public static final String TOPIC_VOUCHER_RELEASE = "voucher-release-events"; // Compensating transaction (Release voucher)
    public static final String TOPIC_FRAUD_ALERTS = "fraud-alerts";
    public static final String TOPIC_ORDER_DLT = "order-events.DLT";
}
