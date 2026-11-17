package com.ecommerce.notification.service;

import com.ecommerce.notification.model.NotificationLog;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@Slf4j
@Service
public class NotificationService {

    private final List<NotificationLog> logs = new CopyOnWriteArrayList<>();

    public void sendNotification(String type, String recipient, String title, String content) {
        NotificationLog notif = new NotificationLog(type, recipient, title, content);
        logs.add(0, notif);
        if (logs.size() > 100) {
            logs.remove(logs.size() - 1);
        }

        log.info("📩 [NotificationService] [{}] Sent to '{}': {} - {}", type, recipient, title, content);
    }

    public List<NotificationLog> getRecentNotifications() {
        return Collections.unmodifiableList(new ArrayList<>(logs));
    }
}
