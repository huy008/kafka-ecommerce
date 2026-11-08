package com.example.ecommerce.service;

import com.example.ecommerce.config.KafkaTopicAndStreamsConfig;
import com.example.ecommerce.dto.FraudAlertEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@Service
public class FraudAlertsListener {

    private static final Logger log = LoggerFactory.getLogger(FraudAlertsListener.class);

    private final List<FraudAlertEvent> alertHistory = new CopyOnWriteArrayList<>();
    private final ObjectMapper objectMapper;
    private final OrderService orderService;

    public FraudAlertsListener(ObjectMapper objectMapper, OrderService orderService) {
        this.objectMapper = objectMapper;
        this.orderService = orderService;
    }

    @KafkaListener(
            topics = KafkaTopicAndStreamsConfig.TOPIC_FRAUD_ALERTS,
            groupId = "fraud-monitoring-dashboard"
    )
    public void onFraudAlert(ConsumerRecord<String, String> record) {
        try {
            FraudAlertEvent alert = objectMapper.readValue(record.value(), FraudAlertEvent.class);
            alertHistory.add(0, alert); // Đưa lên đầu danh sách

            // Giới hạn lịch sử lưu 100 cảnh báo gần nhất
            if (alertHistory.size() > 100) {
                alertHistory.remove(alertHistory.size() - 1);
            }

            log.error("=========================================================================");
            log.error("🛑 [FRAUD ALERT DISPATCHER] ĐÃ NHẬN ĐƯỢC CẢNH BÁO TỪ KAFKA STREAMS!");
            log.error("   -> Mã Alert   : {}", alert.getAlertId());
            log.error("   -> Khách hàng : {}", alert.getCustomerId());
            log.error("   -> Số đơn/30s : {}", alert.getOrderCountInWindow());
            log.error("   -> Tổng tiền  : {} VND", alert.getTotalAmountInWindow());
            log.error("   -> Lý do      : {}", alert.getAlertReason());
            log.error("=========================================================================");

            // Tự động kích hoạt cơ chế phòng thủ: Đánh dấu các đơn hàng của user này là FRAUD_SUSPECTED!
            orderService.flagOrderAsFraudSuspicious(alert.getCustomerId());

        } catch (Exception e) {
            log.error("Lỗi parse FraudAlertEvent: {}", e.getMessage());
        }
    }

    public List<FraudAlertEvent> getRecentAlerts() {
        return Collections.unmodifiableList(new ArrayList<>(alertHistory));
    }

    public void clearAlerts() {
        alertHistory.clear();
    }
}
