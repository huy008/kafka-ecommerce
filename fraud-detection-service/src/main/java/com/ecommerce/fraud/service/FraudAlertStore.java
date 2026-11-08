package com.ecommerce.fraud.service;

import com.ecommerce.common.constant.KafkaTopicConstants;
import com.ecommerce.common.event.FraudAlertEvent;
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
public class FraudAlertStore {

    private static final Logger log = LoggerFactory.getLogger(FraudAlertStore.class);

    private final List<FraudAlertEvent> alertHistory = new CopyOnWriteArrayList<>();
    private final ObjectMapper objectMapper;

    public FraudAlertStore(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = KafkaTopicConstants.TOPIC_FRAUD_ALERTS, groupId = "fraud-detection-dashboard-group")
    public void onFraudAlert(ConsumerRecord<String, String> record) {
        try {
            FraudAlertEvent alert = objectMapper.readValue(record.value(), FraudAlertEvent.class);
            alertHistory.add(0, alert);
            if (alertHistory.size() > 100) {
                alertHistory.remove(alertHistory.size() - 1);
            }
            log.warn("🛑 [FraudAlertStore] Đã lưu cảnh báo: {} | Khách hàng: {} | Lý do: {}",
                    alert.getAlertId(), alert.getCustomerId(), alert.getAlertReason());
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

