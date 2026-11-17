package com.ecommerce.fraud.service;

import com.ecommerce.common.constant.KafkaTopicConstants;
import com.ecommerce.common.event.FraudAlertEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@Slf4j
@Service
@RequiredArgsConstructor
public class FraudAlertStore {

    private final List<FraudAlertEvent> alertHistory = new CopyOnWriteArrayList<>();
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = KafkaTopicConstants.TOPIC_FRAUD_ALERTS, groupId = "fraud-detection-dashboard-group")
    public void onFraudAlert(ConsumerRecord<String, String> record) {
        try {
            FraudAlertEvent alert = objectMapper.readValue(record.value(), FraudAlertEvent.class);
            alertHistory.add(0, alert);
            if (alertHistory.size() > 100) {
                alertHistory.remove(alertHistory.size() - 1);
            }
            log.warn("🛑 [FraudAlertStore] Stored fraud alert: {} | Customer: {} | Reason: {}",
                    alert.getAlertId(), alert.getCustomerId(), alert.getAlertReason());
        } catch (Exception e) {
            log.error("Error parsing FraudAlertEvent: {}", e.getMessage());
        }
    }

    public List<FraudAlertEvent> getRecentAlerts() {
        return Collections.unmodifiableList(new ArrayList<>(alertHistory));
    }

    public void clearAlerts() {
        alertHistory.clear();
    }
}
