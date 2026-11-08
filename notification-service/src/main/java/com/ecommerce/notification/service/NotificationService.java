package com.ecommerce.notification.service;

import com.ecommerce.notification.model.NotificationLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@Service
public class NotificationService {

    private static final Logger log = LoggerFactory.getLogger(NotificationService.class);

    private final List<NotificationLog> logs = new CopyOnWriteArrayList<>();

    public void sendNotification(String type, String recipient, String title, String content) {
        NotificationLog notif = new NotificationLog(type, recipient, title, content);
        logs.add(0, notif);
        if (logs.size() > 100) {
            logs.remove(logs.size() - 1);
        }

        log.info("📩 [NotificationService] [{}] Gửi tới '{}': {} - {}", type, recipient, title, content);
    }

    public List<NotificationLog> getRecentNotifications() {
        return Collections.unmodifiableList(new ArrayList<>(logs));
    }
}

