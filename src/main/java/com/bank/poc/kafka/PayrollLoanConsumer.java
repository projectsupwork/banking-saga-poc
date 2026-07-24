package com.bank.poc.kafka;

import com.bank.poc.event.PayrollLoanRequestedEvent;
import com.bank.poc.exception.PayrollMarginExceededException;
import com.bank.poc.service.AccountService;
import com.bank.poc.service.PayrollLoanService;
import com.bank.poc.sqs.SqsNotificationPublisher;
import io.micronaut.configuration.kafka.annotation.KafkaListener;
import io.micronaut.configuration.kafka.annotation.Topic;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Map;

/**
 * Kafka consumer — Payroll Loan origination SAGA.
 *
 * Reuses the same choreography design as the transfer consumer, with one
 * important structural difference: there is no customer-account debit.
 * "Step 2" here books the contract as a bank liability (not an account in
 * AccountService), and "Step 3" credits the disbursed amount using literally
 * the same {@code AccountService.credit} used by transfers.
 *
 *  SAGA flow:
 *  ┌──────────────────────────────────────────────────────────────────────────┐
 *  │ Kafka → [Step 1: Payroll margin]   → [Step 2: Activate contract]         │
 *  │        → [Step 3: Credit customer] → [Step 4: SQS notification]          │
 *  │                                  ↑                                       │
 *  │  If Step 3 fails: ← CancelContract ──┘                                   │
 *  └──────────────────────────────────────────────────────────────────────────┘
 *
 * Installment collection does NOT go through this consumer — it is simulated
 * by the scheduler in PayrollLoanService.processMonthlyDeductions(), which
 * never debits the checking account (the deduction happens on the
 * payroll/benefit, before the money reaches the customer's account).
 */
@KafkaListener(groupId = "payroll-loan-processor", threads = 2)
public class PayrollLoanConsumer {

    private static final Logger log = LoggerFactory.getLogger(PayrollLoanConsumer.class);

    @Inject
    private PayrollLoanService loanService;

    @Inject
    private AccountService accountService;

    @Inject
    private SqsNotificationPublisher sqsPublisher;

    @Topic("loans.payroll.requested")
    public void process(PayrollLoanRequestedEvent event) {
        log.info("=== PAYROLL LOAN SAGA STARTED | sagaId={} contract={} ===", event.sagaId(), event.contractId());

        String contractId = event.contractId();

        // ── STEP 1: Validate payroll margin ─────────────────────────
        try {
            loanService.validatePayrollMargin(event.monthlyIncome(), event.installmentAmount());
            loanService.recordEvent(contractId, "STEP1_MARGIN_VALIDATED",
                "Payroll margin validated for enrollment " + event.enrollmentId());
        } catch (PayrollMarginExceededException e) {
            log.error("[SAGA-CANCELED] Payroll margin exceeded | contract={} msg={}", contractId, e.getMessage());
            loanService.cancelContract(contractId, "Payroll margin exceeded: " + e.getMessage());
            return;
        }

        // ── STEP 2: Activate contract (compensable checkpoint) ──────
        try {
            loanService.activateContract(contractId);
        } catch (Exception e) {
            log.error("[SAGA-CANCELED] Error activating contract | sagaId={}", event.sagaId(), e);
            loanService.cancelContract(contractId, "Error activating contract: " + e.getMessage());
            return;
        }

        // ── STEP 3: Credit customer (reuses the transfer's Step 3) ──
        try {
            var newBalance = accountService.credit(event.customerAccount(), event.requestedAmount());
            log.info("[SAGA-STEP-3] ✓ Disbursement credited | account={} balance={}", event.customerAccount(), newBalance);
            loanService.recordEvent(contractId, "STEP3_DISBURSEMENT",
                "Credited $" + event.requestedAmount() + " to " + event.customerAccount() + " (balance: $" + newBalance + ")");
        } catch (Exception e) {
            log.error("[SAGA-COMPENSATING] Error crediting customer, canceling contract | sagaId={}", event.sagaId(), e);
            loanService.cancelContract(contractId, "Credit failed — contract canceled: " + e.getMessage());
            return;
        }

        // ── STEP 4: Notify customer via SQS ─────────────────────────
        try {
            Map<String, Object> notification = Map.of(
                "contractId",        contractId,
                "sagaId",            event.sagaId(),
                "type",              "PAYROLL_LOAN_ORIGINATED",
                "customerAccount",   event.customerAccount(),
                "enrollmentId",      event.enrollmentId(),
                "amount",            event.requestedAmount(),
                "installmentAmount", event.installmentAmount(),
                "termMonths",        event.termMonths(),
                "message",           "Payroll loan originated! A monthly installment of $%s will be deducted from your benefit for %d months"
                    .formatted(event.installmentAmount(), event.termMonths()),
                "timestamp",         Instant.now().toString()
            );
            sqsPublisher.publish(notification);
            log.info("[SAGA-STEP-4] ✓ SQS notification sent | contract={}", contractId);
            loanService.recordEvent(contractId, "STEP4_SQS", "Origination notification sent via SQS");
        } catch (Exception e) {
            log.error("[SAGA-WARN] SQS notification failed (origination OK) | contract={}", contractId, e);
            loanService.recordEvent(contractId, "STEP4_SQS_FAILED", "SQS notification failed (origination completed)");
        }

        log.info("=== PAYROLL LOAN SAGA COMPLETED ✓ | contract={} account={} amount={} ===",
            contractId, event.customerAccount(), event.requestedAmount());
    }
}
