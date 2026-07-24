package com.bank.poc.transfer.domain.event;

import io.micronaut.serde.annotation.Serdeable;
import java.math.BigDecimal;
import java.time.Instant;

/** Published to transfers.processing after a successful debit. */
@Serdeable
public record DebitCompletedEvent(
    String sagaId,
    String transferId,
    String type,
    String account,
    BigDecimal amount,
    BigDecimal previousBalance,
    BigDecimal currentBalance,
    String sagaState,
    Instant timestamp
) {}
