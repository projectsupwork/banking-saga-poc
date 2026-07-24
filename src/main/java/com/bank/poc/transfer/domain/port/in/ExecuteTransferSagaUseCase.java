package com.bank.poc.transfer.domain.port.in;

import com.bank.poc.transfer.domain.event.TransferRequestedEvent;

/**
 * Inbound port (driving port) — executes the SAGA choreography steps for a
 * requested transfer. Implemented by the application layer, consumed by the
 * messaging adapter (Kafka listener).
 */
public interface ExecuteTransferSagaUseCase {
    void execute(TransferRequestedEvent event);
}
