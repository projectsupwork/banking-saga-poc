package com.bank.poc.transfer.adapter.out.messaging;

import com.bank.poc.transfer.domain.event.TransferRequestedEvent;
import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.configuration.kafka.annotation.KafkaKey;
import io.micronaut.configuration.kafka.annotation.Topic;
import reactor.core.publisher.Mono;

/**
 * Kafka client for the transfers topic.
 *
 * @KafkaClient generates a compile-time proxy via Micronaut AOT.
 * The key (sagaId) guarantees that every message of a given SAGA lands on
 * the same partition — preserving event ordering.
 */
@KafkaClient(id = "transfers-producer")
public interface TransferProducer {

    @Topic("transfers.requested")
    Mono<Void> publishTransfer(
        @KafkaKey String sagaId,
        TransferRequestedEvent event
    );
}
