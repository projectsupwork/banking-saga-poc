package com.bank.poc.transfer.domain;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Transfer-completed notification to be delivered to the recipient.
 * Infrastructure-neutral representation — the outbound adapter decides how
 * to serialize/transport it (today: SQS).
 */
public record Notification(
    String sagaId,
    String transferId,
    String type,
    String recipient,
    String sender,
    BigDecimal amount,
    String message,
    Instant timestamp
) {
    public static Notification ofCompletedTransfer(
        String sagaId, String transferId, String recipient, String sender, BigDecimal amount) {
        return new Notification(
            sagaId, transferId, "TRANSFER_NOTIFICATION",
            recipient, sender, amount,
            "You received $%s from account %s".formatted(amount, sender),
            Instant.now()
        );
    }
}
