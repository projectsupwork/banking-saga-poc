package com.bank.poc.transfer.domain;

import com.bank.poc.transfer.domain.exception.InvalidTransferException;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * Aggregate root of the transfer domain.
 *
 * Encapsulates the single synchronous business rule (source != target) and
 * the generation of traceable identifiers: {@code protocolId} (trace ID
 * exposed to the client) and {@code sagaId} (correlates the SAGA steps).
 */
public record Transfer(
    String protocolId,
    String sagaId,
    String sourceAccount,
    String targetAccount,
    BigDecimal amount
) {

    public Transfer {
        if (sourceAccount.equals(targetAccount)) {
            throw new InvalidTransferException(
                "Source and target accounts must differ: " + sourceAccount);
        }
    }

    public static Transfer request(String sourceAccount, String targetAccount, BigDecimal amount) {
        String protocolId = "TRF-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        String sagaId = "SAGA-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        return new Transfer(protocolId, sagaId, sourceAccount, targetAccount, amount);
    }
}
