package com.example.ecommerce.service;

import com.example.ecommerce.dto.CreateOrderRequest;
import com.example.ecommerce.dto.OrderEventDTO;
import com.example.ecommerce.entity.OrderEntity;
import com.example.ecommerce.entity.OutboxEventEntity;
import com.example.ecommerce.repository.OrderRepository;
import com.example.ecommerce.repository.OutboxEventRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    private final OrderRepository orderRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    public OrderService(OrderRepository orderRepository,
                        OutboxEventRepository outboxEventRepository,
                        ObjectMapper objectMapper) {
        this.orderRepository = orderRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.objectMapper = objectMapper;
    }

    /**
     * TRANSACTIONAL OUTBOX PATTERN:
     * Lưu Đơn hàng và Bản tin sự kiện vào CÙNG 1 TRANSACTION SQL duy nhất.
     * Dù Kafka Broker có bị sập lúc này, đơn hàng vẫn được lưu an toàn
     * và sự kiện PENDING sẽ được OutboxPublisher quét gửi lại sau khi Kafka hồi phục!
     */
    @Transactional
    public OrderEntity createOrder(CreateOrderRequest request) {
        String orderNumber = "ORD-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        String eventId = "EVT-" + UUID.randomUUID().toString();

        int quantity = (request.getQuantity() != null && request.getQuantity() > 0) ? request.getQuantity() : 1;
        Double totalAmount = request.getAmount();
        Double unitPrice = request.getUnitPrice() != null ? request.getUnitPrice() : (totalAmount != null ? totalAmount / quantity : 0.0);
        String paymentMethod = request.getPaymentMethod() != null ? request.getPaymentMethod() : "CREDIT_CARD";
        String shippingAddress = request.getShippingAddress() != null ? request.getShippingAddress() : "Hà Nội, Việt Nam";

        log.info("🛍️ [OrderService] Đang tạo đơn hàng: {} cho khách hàng: {} | SP: {} | Số lượng: {} | Tổng tiền: {} VND",
                orderNumber, request.getCustomerId(), request.getProductName(), quantity, totalAmount);

        // 1. Lưu đơn hàng vào bảng orders
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

        // 2. Tạo Payload sự kiện giàu ngữ cảnh (Rich Event Payload)
        OrderEventDTO eventPayload = new OrderEventDTO(
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

            // 3. Lưu sự kiện vào bảng outbox_events trong CÙNG TRANSACTION SQL
            OutboxEventEntity outboxEvent = new OutboxEventEntity(
                    eventId,
                    "ORDER",
                    orderNumber,
                    "ORDER_CREATED",
                    jsonPayload
            );
            outboxEventRepository.save(outboxEvent);

            log.info("💾 [Transactional Outbox] Đã ghi nhận Order & Outbox Event ({}) vào Database trong cùng 1 Transaction!", eventId);

        } catch (JsonProcessingException e) {
            throw new RuntimeException("Lỗi serialize event payload", e);
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
            log.info("🔄 [Order Service] Cập nhật đơn {} -> Trạng thái: {}, Lý do: {}", orderNumber, status, failureReason);
        });
    }

    @Transactional
    public void flagOrderAsFraudSuspicious(String customerId) {
        List<OrderEntity> orders = orderRepository.findByCustomerIdOrderByCreatedAtDesc(customerId);
        for (OrderEntity order : orders) {
            if ("PENDING".equals(order.getStatus()) || "CONFIRMED".equals(order.getStatus())) {
                order.setFraudSuspected(true);
                order.setStatus("FRAUD_SUSPECTED");
                order.setFailureReason("Kafka Streams phát hiện dấu hiệu gian lận tần suất cao!");
                orderRepository.save(order);
                log.warn("🚨 [Fraud Guard] Đã khóa cảnh báo đơn hàng {} của khách hàng {}", order.getOrderNumber(), customerId);
            }
        }
    }
}
