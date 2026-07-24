package com.bank.poc.transfer.adapter.in.web.dto;

import com.bank.poc.transfer.domain.Transfer;
import io.micronaut.serde.annotation.Serdeable;
import java.time.Instant;

@Serdeable
public record TransferResponse(
    String protocolId,
    String status,
    String message,
    Instant timestamp
) {
    public static TransferResponse accepted(Transfer transfer) {
        return new TransferResponse(
            transfer.protocolId(),
            "PROCESSING",
            "Transfer received and being processed asynchronously",
            Instant.now()
        );
    }
}
