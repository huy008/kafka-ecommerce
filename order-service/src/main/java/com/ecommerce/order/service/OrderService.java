package com.ecommerce.order.service;

import com.ecommerce.common.event.OrderCreatedEvent;
import com.ecommerce.order.dto.CreateOrderRequest;
import com.ecommerce.order.entity.OrderEntity;
import com.ecommerce.order.entity.OutboxEventEntity;
import com.ecommerce.order.repository.OrderRepository;
import com.ecommerce.order.repository.OutboxEventRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderService {

    private final OrderRepository orderRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    /**
     * TRANSACTIONAL OUTBOX PATTERN:
     * Saves both Order and OutboxEvent in the EXACT SAME SQL TRANSACTION.
     */
    @Transactional
    public OrderEntity createOrder(CreateOrderRequest request) {
        String orderNumber = "ORD-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        String eventId = "EVT-" + UUID.randomUUID().toString();

        int quantity = (request.getQuantity() != null && request.getQuantity() > 0) ? request.getQuantity() : 1;
        Double totalAmount = request.getAmount();
        Double unitPrice = request.getUnitPrice() != null ? request.getUnitPrice() : (totalAmount != null ? totalAmount / quantity : 0.0);
        String paymentMethod = request.getPaymentMethod() != null ? request.getPaymentMethod() : "CREDIT_CARD";
        String shippingAddress = request.getShippingAddress() != null ? request.getShippingAddress() : "Hanoi, Vietnam";

        log.info("🛍️ [OrderService] Creating order: {} for customer: {} | Product: {} | Qty: {} | Total: {} VND",
                orderNumber, request.getCustomerId(), request.getProductName(), quantity, totalAmount);

        // 1. Save Order into orders table with PENDING status
        OrderEntity order = new OrderEntity(
                orderNumber,
                request.getCustomerId(),
                request.getProductName(),
                quantity,
                unitPrice,
                totalAmount,
                paymentMethod,
                shippingAddress,
                "PENDING"
        );
        OrderEntity savedOrder = orderRepository.save(order);

        // 2. Build Event DTO
        OrderCreatedEvent eventPayload = new OrderCreatedEvent(
                eventId,
                orderNumber,
                request.getCustomerId(),
                request.getProductName(),
                quantity,
                unitPrice,
                totalAmount,
                paymentMethod,
                shippingAddress
        );

        try {
            String jsonPayload = objectMapper.writeValueAsString(eventPayload);

            // 3. Save into outbox_events within the SAME SQL TRANSACTION
            OutboxEventEntity outboxEvent = new OutboxEventEntity(
                    eventId,
                    "ORDER",
                    orderNumber,
                    "ORDER_CREATED",
                    jsonPayload
            );
            outboxEventRepository.save(outboxEvent);

            log.info("💾 [Transactional Outbox] Successfully persisted Order & Outbox Event ({}) to Database!", eventId);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Error serializing event payload", e);
        }

        return savedOrder;
    }

    public List<OrderEntity> getAllOrders() {
        return orderRepository.findAll();
    }

    public List<OrderEntity> getRecentOrders() {
        return orderRepository.findTop50ByOrderByCreatedAtDesc();
    }

    public Optional<OrderEntity> getOrderByOrderNumber(String orderNumber) {
        return orderRepository.findByOrderNumber(orderNumber);
    }

    @Transactional
    public void updateOrderStatus(String orderNumber, String status, String failureReason) {
        orderRepository.findByOrderNumber(orderNumber).ifPresent(order -> {
            order.setStatus(status);
            if (failureReason != null) {
                order.setFailureReason(failureReason);
            }
            orderRepository.save(order);
            log.info("🔄 [OrderService] Updated order {} -> Status: {}, Reason: {}", orderNumber, status, failureReason);
        });
    }

    @Transactional
    public void flagOrderAsFraudSuspicious(String customerId) {
        List<OrderEntity> orders = orderRepository.findByCustomerIdOrderByCreatedAtDesc(customerId);
        for (OrderEntity order : orders) {
            if ("PENDING".equals(order.getStatus()) || "INVENTORY_RESERVED".equals(order.getStatus()) || "CONFIRMED".equals(order.getStatus())) {
                order.setFraudSuspected(true);
                order.setStatus("FRAUD_SUSPECTED");
                order.setFailureReason("Kafka Streams detected high-frequency fraudulent behavior!");
                orderRepository.save(order);
                log.warn("🚨 [Fraud Guard] Flagged and locked order {} for customer {}", order.getOrderNumber(), customerId);
            }
        }
    }
}
