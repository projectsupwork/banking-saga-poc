package com.bank.poc.transfer.domain.port.out;

import com.bank.poc.transfer.domain.Notification;

/**
 * Outbound port (driven port) — asynchronous delivery of the notification to
 * the recipient of a completed transfer. Failure here is non-fatal (the
 * transfer is already committed); the adapter decides how to retry.
 */
public interface NotificationPublisherPort {
    void publish(Notification notification);
}
