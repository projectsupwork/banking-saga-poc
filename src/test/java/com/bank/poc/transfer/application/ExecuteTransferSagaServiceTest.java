package com.bank.poc.transfer.application;

import com.bank.poc.exception.InsufficientBalanceException;
import com.bank.poc.transfer.domain.Notification;
import com.bank.poc.transfer.domain.event.TransferRequestedEvent;
import com.bank.poc.transfer.domain.port.out.AccountGatewayPort;
import com.bank.poc.transfer.domain.port.out.NotificationPublisherPort;
import com.bank.poc.transfer.domain.port.out.SagaTrackerPort;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.math.BigDecimal;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Unit tests for the transfer SAGA choreography — outbound ports mocked,
 * covering the happy path, cancellation and compensation.
 */
class ExecuteTransferSagaServiceTest {

    private AccountGatewayPort accountGateway;
    private NotificationPublisherPort notificationPublisher;
    private SagaTrackerPort sagaTracker;
    private ExecuteTransferSagaService service;

    private static final BigDecimal AMOUNT = new BigDecimal("500.00");

    @BeforeEach
    void setUp() {
        accountGateway = mock(AccountGatewayPort.class);
        notificationPublisher = mock(NotificationPublisherPort.class);
        sagaTracker = mock(SagaTrackerPort.class);
        service = new ExecuteTransferSagaService(accountGateway, notificationPublisher, sagaTracker);
    }

    private TransferRequestedEvent event() {
        return new TransferRequestedEvent(
            "TRF-1", "SAGA-1", "TRANSFER_REQUESTED",
            "ACC-001", "ACC-002", AMOUNT, "USD", "STARTED", java.time.Instant.now());
    }

    @Test
    @DisplayName("Happy path: validates, debits, credits and notifies")
    void executesSagaSuccessfully() {
        when(accountGateway.debit("ACC-001", AMOUNT)).thenReturn(new BigDecimal("4500.00"));
        when(accountGateway.credit("ACC-002", AMOUNT)).thenReturn(new BigDecimal("1500.00"));

        service.execute(event());

        InOrder order = inOrder(accountGateway, notificationPublisher, sagaTracker);
        order.verify(accountGateway).validateBalance("ACC-001", AMOUNT);
        order.verify(accountGateway).debit("ACC-001", AMOUNT);
        order.verify(accountGateway).credit("ACC-002", AMOUNT);
        order.verify(notificationPublisher).publish(any(Notification.class));
        order.verify(sagaTracker).complete("TRF-1");

        verify(accountGateway, never()).creditCompensation(anyString(), any());
    }

    @Test
    @DisplayName("Insufficient balance cancels the SAGA without debiting")
    void cancelsSagaOnInsufficientBalance() {
        doThrow(new InsufficientBalanceException("insufficient balance"))
            .when(accountGateway).validateBalance("ACC-001", AMOUNT);

        service.execute(event());

        verify(accountGateway, never()).debit(anyString(), any());
        verify(sagaTracker).cancel(eq("TRF-1"), anyString());
    }

    @Test
    @DisplayName("Credit error triggers compensation (reverts the debit)")
    void compensatesDebitWhenCreditFails() {
        when(accountGateway.debit("ACC-001", AMOUNT)).thenReturn(new BigDecimal("4500.00"));
        when(accountGateway.credit("ACC-002", AMOUNT)).thenThrow(new RuntimeException("credit failure"));

        service.execute(event());

        verify(accountGateway).creditCompensation("ACC-001", AMOUNT);
        verify(sagaTracker).cancel(eq("TRF-1"), anyString());
        verify(notificationPublisher, never()).publish(any());
    }

    @Test
    @DisplayName("Notification failure does not prevent the SAGA from completing")
    void notificationFailureDoesNotPreventCompletion() {
        when(accountGateway.debit("ACC-001", AMOUNT)).thenReturn(new BigDecimal("4500.00"));
        when(accountGateway.credit("ACC-002", AMOUNT)).thenReturn(new BigDecimal("1500.00"));
        doThrow(new RuntimeException("SQS unavailable")).when(notificationPublisher).publish(any());

        service.execute(event());

        verify(sagaTracker).event(eq("TRF-1"), eq("STEP4_SQS_FAILED"), anyString());
        verify(sagaTracker).complete("TRF-1");
    }
}
