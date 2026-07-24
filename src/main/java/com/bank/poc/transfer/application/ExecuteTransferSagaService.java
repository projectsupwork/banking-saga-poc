package com.bank.poc.transfer.application;

import com.bank.poc.exception.InsufficientBalanceException;
import com.bank.poc.transfer.domain.Notification;
import com.bank.poc.transfer.domain.event.TransferRequestedEvent;
import com.bank.poc.transfer.domain.port.in.ExecuteTransferSagaUseCase;
import com.bank.poc.transfer.domain.port.out.AccountGatewayPort;
import com.bank.poc.transfer.domain.port.out.NotificationPublisherPort;
import com.bank.poc.transfer.domain.port.out.SagaTrackerPort;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;

/**
 * Use case: execute the SAGA choreography of a transfer.
 *
 * Banking analogy: think of this use case as the "clearing house operator"
 * that processes wire transfers in batch. It reads the event, executes the
 * steps and, if anything fails, undoes what was done (just like a bank's
 * automatic reversal).
 *
 *  SAGA flow:
 *  ┌────────────────────────────────────────────────────────────────────────────────┐
 *  │  Event → [Step 1: Validate] → [Step 2: Debit] → [Step 3: Credit] → [Notify]    │
 *  │                                       ↑                                        │
 *  │  If Step 3 fails: ← RevertDebit ──────┘                                        │
 *  └────────────────────────────────────────────────────────────────────────────────┘
 */
@Singleton
public class ExecuteTransferSagaService implements ExecuteTransferSagaUseCase {

    private static final Logger log = LoggerFactory.getLogger(ExecuteTransferSagaService.class);

    private final AccountGatewayPort accountGateway;
    private final NotificationPublisherPort notificationPublisher;
    private final SagaTrackerPort sagaTracker;

    public ExecuteTransferSagaService(
        AccountGatewayPort accountGateway,
        NotificationPublisherPort notificationPublisher,
        SagaTrackerPort sagaTracker
    ) {
        this.accountGateway = accountGateway;
        this.notificationPublisher = notificationPublisher;
        this.sagaTracker = sagaTracker;
    }

    @Override
    public void execute(TransferRequestedEvent event) {
        log.info("=== SAGA STARTED | sagaId={} trf={} ===", event.sagaId(), event.id());

        String sagaId = event.sagaId();
        String tId = event.id();
        String source = event.sourceAccount();
        String target = event.targetAccount();
        BigDecimal amount = event.amount();

        // ── STEP 1: Validate balance ────────────────────────────────
        try {
            accountGateway.validateBalance(source, amount);
            sagaTracker.event(tId, "STEP1_VALIDATION", "Balance validated on " + source);
        } catch (InsufficientBalanceException e) {
            log.error("[SAGA-CANCELED] Insufficient balance | sagaId={} msg={}", sagaId, e.getMessage());
            sagaTracker.cancel(tId, "Insufficient balance: " + e.getMessage());
            publishCancellation(sagaId, "INSUFFICIENT_BALANCE", e.getMessage());
            return;
        }

        // ── STEP 2: Debit source (compensable checkpoint) ───────────
        BigDecimal newSourceBalance;
        try {
            newSourceBalance = accountGateway.debit(source, amount);
            log.info("[SAGA-STEP-2] ✓ Debit completed | account={} balance={}", source, newSourceBalance);
            sagaTracker.event(tId, "STEP2_DEBIT",
                "Debited $" + amount + " from " + source + " (balance: $" + newSourceBalance + ")");
        } catch (Exception e) {
            log.error("[SAGA-CANCELED] Debit error | sagaId={}", sagaId, e);
            sagaTracker.cancel(tId, "Debit error: " + e.getMessage());
            publishCancellation(sagaId, "DEBIT_ERROR", e.getMessage());
            return;
        }

        // ── STEP 3: Credit target ───────────────────────────────────
        BigDecimal newTargetBalance;
        try {
            newTargetBalance = accountGateway.credit(target, amount);
            log.info("[SAGA-STEP-3] ✓ Credit completed | account={} balance={}", target, newTargetBalance);
            sagaTracker.event(tId, "STEP3_CREDIT",
                "Credited $" + amount + " to " + target + " (balance: $" + newTargetBalance + ")");
        } catch (Exception e) {
            log.error("[SAGA-COMPENSATING] Credit error, reverting debit | sagaId={}", sagaId, e);
            try {
                accountGateway.creditCompensation(source, amount);
                log.info("[SAGA-COMP] ✓ Debit reverted | account={}", source);
                sagaTracker.cancel(tId, "Credit failed — debit reverted on " + source);
                publishCancellation(sagaId, "CREDIT_ERROR_DEBIT_REVERTED", e.getMessage());
            } catch (Exception compEx) {
                log.error("[SAGA-CRITICAL] Compensation failed! | sagaId={}", sagaId, compEx);
                sagaTracker.fail(tId, "CRITICAL FAILURE: compensation failed — manual intervention required");
                publishCancellation(sagaId, "CRITICAL_COMPENSATION_FAILURE", compEx.getMessage());
            }
            return;
        }

        // ── STEP 4: Notify recipient ────────────────────────────────
        try {
            notificationPublisher.publish(
                Notification.ofCompletedTransfer(sagaId, tId, target, source, amount));
            log.info("[SAGA-STEP-4] ✓ Notification sent | sagaId={}", sagaId);
            sagaTracker.event(tId, "STEP4_SQS", "Notification sent to recipient via SQS");
        } catch (Exception e) {
            log.error("[SAGA-WARN] Failed to notify recipient (transfer OK) | sagaId={}", sagaId, e);
            sagaTracker.event(tId, "STEP4_SQS_FAILED", "SQS notification failed (transfer completed)");
        }

        sagaTracker.complete(tId);
        log.info("=== SAGA COMPLETED ✓ | sagaId={} source={} target={} amount={} ===",
            sagaId, source, target, amount);
    }

    private void publishCancellation(String sagaId, String reason, String detail) {
        log.warn("[SAGA-CANCELED] sagaId={} reason={} detail={}", sagaId, reason, detail);
        // In production: publish to transfers.canceled via a dedicated outbound
        // port for auditing and monitoring dashboards
    }
}
