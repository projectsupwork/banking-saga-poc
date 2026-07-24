package com.bank.poc.transfer.domain.event;

import com.bank.poc.transfer.domain.Transfer;
import io.micronaut.serde.annotation.Serdeable;
import java.math.BigDecimal;
import java.time.Instant;

/**
 * Domain event published to the "transfers.requested" topic.
 * Triggers the SAGA choreography in the asynchronous inbound listener.
 */
@Serdeable
public record TransferRequestedEvent(
    String id,               // unique transfer ID (protocolId)
    String sagaId,           // unique SAGA execution ID
    String type,             // TRANSFER_REQUESTED
    String sourceAccount,
    String targetAccount,
    BigDecimal amount,
    String currency,         // USD
    String sagaState,        // STARTED
    Instant timestamp
) {
    public static TransferRequestedEvent of(Transfer transfer) {
        return new TransferRequestedEvent(
            transfer.protocolId(), transfer.sagaId(), "TRANSFER_REQUESTED",
            transfer.sourceAccount(), transfer.targetAccount(), transfer.amount(),
            "USD", "STARTED", Instant.now()
        );
    }
}
