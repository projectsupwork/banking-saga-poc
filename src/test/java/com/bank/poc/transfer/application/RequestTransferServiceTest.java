package com.bank.poc.transfer.application;

import com.bank.poc.transfer.domain.Transfer;
import com.bank.poc.transfer.domain.event.TransferRequestedEvent;
import com.bank.poc.transfer.domain.exception.InvalidTransferException;
import com.bank.poc.transfer.domain.port.out.SagaTrackerPort;
import com.bank.poc.transfer.domain.port.out.TransferEventPublisherPort;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Unit tests for the request-transfer use case — outbound ports mocked,
 * no real infrastructure (Kafka/SQS).
 */
class RequestTransferServiceTest {

    private TransferEventPublisherPort eventPublisher;
    private SagaTrackerPort sagaTracker;
    private RequestTransferService service;

    @BeforeEach
    void setUp() {
        eventPublisher = mock(TransferEventPublisherPort.class);
        sagaTracker = mock(SagaTrackerPort.class);
        service = new RequestTransferService(eventPublisher, sagaTracker);
    }

    @Test
    @DisplayName("A valid request registers the SAGA and publishes the event")
    void requestsValidTransfer() {
        Transfer transfer = service.request("ACC-001", "ACC-002", new BigDecimal("500.00"));

        assertThat(transfer.protocolId()).startsWith("TRF-");
        assertThat(transfer.sagaId()).startsWith("SAGA-");

        verify(sagaTracker).register(transfer.protocolId(), transfer.sagaId(),
            "ACC-001", "ACC-002", new BigDecimal("500.00"));
        verify(sagaTracker).event(eq(transfer.protocolId()), eq("SAGA_STARTED"), any());

        ArgumentCaptor<TransferRequestedEvent> captor = ArgumentCaptor.forClass(TransferRequestedEvent.class);
        verify(eventPublisher).publish(captor.capture());
        assertThat(captor.getValue().id()).isEqualTo(transfer.protocolId());
        assertThat(captor.getValue().sagaId()).isEqualTo(transfer.sagaId());
    }

    @Test
    @DisplayName("Self-transfer (source == target) throws InvalidTransferException and publishes nothing")
    void rejectsInvalidSelfTransfer() {
        assertThatThrownBy(() -> service.request("ACC-001", "ACC-001", new BigDecimal("500.00")))
            .isInstanceOf(InvalidTransferException.class)
            .isInstanceOf(IllegalArgumentException.class);

        verifyNoInteractions(eventPublisher, sagaTracker);
    }
}
