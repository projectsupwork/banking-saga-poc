package com.bank.poc.transfer.adapter.out.notification;

import com.bank.poc.sqs.SqsNotificationPublisher;
import com.bank.poc.transfer.domain.Notification;
import com.bank.poc.transfer.domain.port.out.NotificationPublisherPort;
import jakarta.inject.Singleton;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Outbound adapter that implements {@link NotificationPublisherPort} by
 * translating the {@link Notification} domain object into the format accepted
 * by the shared {@link SqsNotificationPublisher} (SQS via LocalStack).
 */
@Singleton
public class SqsNotificationPublisherAdapter implements NotificationPublisherPort {

    private final SqsNotificationPublisher sqsPublisher;

    public SqsNotificationPublisherAdapter(SqsNotificationPublisher sqsPublisher) {
        this.sqsPublisher = sqsPublisher;
    }

    @Override
    public void publish(Notification notification) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("sagaId", notification.sagaId());
        body.put("transferId", notification.transferId());
        body.put("type", notification.type());
        body.put("recipient", notification.recipient());
        body.put("sender", notification.sender());
        body.put("amount", notification.amount());
        body.put("message", notification.message());
        body.put("timestamp", notification.timestamp().toString());
        sqsPublisher.publish(body);
    }
}
