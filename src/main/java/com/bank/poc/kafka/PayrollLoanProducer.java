package com.bank.poc.kafka;

import com.bank.poc.event.PayrollLoanRequestedEvent;
import io.micronaut.configuration.kafka.annotation.KafkaClient;
import io.micronaut.configuration.kafka.annotation.KafkaKey;
import io.micronaut.configuration.kafka.annotation.Topic;
import reactor.core.publisher.Mono;

/**
 * Kafka producer for the payroll loan origination topic.
 *
 * Same pattern as TransferProducer: the key (sagaId) guarantees that every
 * message of a given SAGA lands on the same partition.
 */
@KafkaClient(id = "payroll-loan-producer")
public interface PayrollLoanProducer {

    @Topic("loans.payroll.requested")
    Mono<Void> publishLoanRequest(
        @KafkaKey String sagaId,
        PayrollLoanRequestedEvent event
    );
}
