package com.bank.poc.transfer.domain.port.out;

import com.bank.poc.transfer.domain.event.TransferRequestedEvent;

/**
 * Outbound port (driven port) — publishes the event that triggers the SAGA.
 * The application layer knows nothing about Kafka or reactive types: the
 * concrete adapter owns every transport/asynchrony detail.
 */
public interface TransferEventPublisherPort {
    void publish(TransferRequestedEvent event);
}
