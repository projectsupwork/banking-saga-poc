package com.bank.poc.transfer.adapter.out.messaging;

import com.bank.poc.transfer.domain.event.TransferRequestedEvent;
import com.bank.poc.transfer.domain.port.out.TransferEventPublisherPort;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Outbound adapter that implements {@link TransferEventPublisherPort} by
 * publishing to Kafka via {@link TransferProducer}. Confines the reactive
 * type (Mono) and the publish-failure logging to this infrastructure edge —
 * the application layer only sees the port.
 */
@Singleton
public class KafkaTransferEventPublisher implements TransferEventPublisherPort {

    private static final Logger log = LoggerFactory.getLogger(KafkaTransferEventPublisher.class);

    private final TransferProducer producer;

    public KafkaTransferEventPublisher(TransferProducer producer) {
        this.producer = producer;
    }

    @Override
    public void publish(TransferRequestedEvent event) {
        producer.publishTransfer(event.sagaId(), event)
            .doOnError(e -> log.error("Failed to publish to Kafka | protocol={}", event.id(), e))
            .subscribe();
    }
}
