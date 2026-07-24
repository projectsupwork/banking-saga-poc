package com.bank.poc.transfer.adapter.in.messaging;

import com.bank.poc.transfer.domain.event.TransferRequestedEvent;
import com.bank.poc.transfer.domain.port.in.ExecuteTransferSagaUseCase;
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.Topic;
import jakarta.inject.Inject;

/**
 * Inbound adapter (driving adapter) that consumes the requested-transfers
 * Kafka topic and delegates SAGA execution to the domain use case.
 * No business logic — transport translation only.
 */
@KafkaListener(groupId = "transfer-processor", threads = 3)
public class TransferKafkaListener {

    @Inject
    private ExecuteTransferSagaUseCase executeSaga;

    @Topic("transfers.requested")
    public void process(TransferRequestedEvent event) {
        executeSaga.execute(event);
    }
}
