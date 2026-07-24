package com.bank.poc.transfer.application;

import com.bank.poc.transfer.domain.Transfer;
import com.bank.poc.transfer.domain.event.TransferRequestedEvent;
import com.bank.poc.transfer.domain.port.in.RequestTransferUseCase;
import com.bank.poc.transfer.domain.port.out.SagaTrackerPort;
import com.bank.poc.transfer.domain.port.out.TransferEventPublisherPort;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;

/**
 * Use case: request a transfer.
 *
 * Responsibilities:
 *  1. Delegate synchronous validation to the {@link Transfer} aggregate
 *  2. Register the SAGA start for auditing
 *  3. Publish the domain event (handoff to asynchronous processing)
 *  4. Return immediately — never blocking the response
 *
 * The real processing (debit/credit/notification) happens in
 * {@link ExecuteTransferSagaService}.
 */
@Singleton
public class RequestTransferService implements RequestTransferUseCase {

    private static final Logger log = LoggerFactory.getLogger(RequestTransferService.class);

    private final TransferEventPublisherPort eventPublisher;
    private final SagaTrackerPort sagaTracker;

    public RequestTransferService(TransferEventPublisherPort eventPublisher, SagaTrackerPort sagaTracker) {
        this.eventPublisher = eventPublisher;
        this.sagaTracker = sagaTracker;
    }

    @Override
    public Transfer request(String sourceAccount, String targetAccount, BigDecimal amount) {
        Transfer transfer = Transfer.request(sourceAccount, targetAccount, amount);

        log.info("Transfer requested | protocol={} source={} target={} amount={}",
            transfer.protocolId(), sourceAccount, targetAccount, amount);

        sagaTracker.register(transfer.protocolId(), transfer.sagaId(), sourceAccount, targetAccount, amount);
        sagaTracker.event(transfer.protocolId(), "SAGA_STARTED", "Event published to Kafka — SAGA in progress");

        eventPublisher.publish(TransferRequestedEvent.of(transfer));

        return transfer;
    }
}
