package com.bank.poc.service;

import com.bank.poc.dto.PayrollLoanRequest;
import com.bank.poc.dto.PayrollLoanResponse;
import com.bank.poc.event.PayrollLoanRequestedEvent;
import com.bank.poc.exception.ContractNotFoundException;
import com.bank.poc.exception.PayrollMarginExceededException;
import com.bank.poc.kafka.PayrollLoanProducer;
import com.bank.poc.model.ContractStatus;
import com.bank.poc.sqs.SqsNotificationPublisher;
import io.micronaut.scheduling.annotation.Scheduled;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Payroll Loan domain.
 *
 * Key difference from a regular transfer: the disbursed money does not leave a
 * customer account (there is no "debit" Step 2 here) — it comes from the
 * bank's own capital. And installment collection is not a checking-account
 * debit: it is deducted straight from the payroll/benefit, simulated here by
 * {@link #processMonthlyDeductions()}, which never calls
 * {@code AccountService.debit}.
 *
 * Rules modeled after a real payroll loan product:
 *  - fixed rate of 1.54% per month
 *  - term between 24 and 120 months
 *  - payroll margin of 35% of monthly income (installment cap)
 */
@Singleton
public class PayrollLoanService {

    private static final Logger log = LoggerFactory.getLogger(PayrollLoanService.class);

    private static final BigDecimal MONTHLY_RATE = new BigDecimal("0.0154");
    private static final BigDecimal MAX_PAYROLL_MARGIN = new BigDecimal("0.35");
    private static final int MAX_HISTORY = 50;

    private final Map<String, PayrollLoanContract> contracts = new ConcurrentHashMap<>();
    private final Deque<String> order = new ConcurrentLinkedDeque<>();

    private final PayrollLoanProducer producer;
    private final SqsNotificationPublisher sqsPublisher;

    public PayrollLoanService(PayrollLoanProducer producer, SqsNotificationPublisher sqsPublisher) {
        this.producer = producer;
        this.sqsPublisher = sqsPublisher;
    }

    /**
     * Synchronous entry point — equivalent to RequestTransferService.
     * Computes the installment, generates the IDs, registers the contract as
     * AWAITING_DISBURSEMENT and publishes the event that triggers the SAGA in
     * PayrollLoanConsumer.
     */
    public PayrollLoanResponse startOrigination(PayrollLoanRequest request) {
        boolean enrollmentHasOpenContract = contracts.values().stream()
            .anyMatch(c -> c.enrollmentId.equals(request.enrollmentId())
                && (c.status == ContractStatus.ACTIVE || c.status == ContractStatus.AWAITING_DISBURSEMENT));
        if (enrollmentHasOpenContract) {
            throw new IllegalArgumentException(
                "Enrollment " + request.enrollmentId() + " already has an open payroll loan contract");
        }

        BigDecimal installmentAmount = computeInstallment(request.requestedAmount(), MONTHLY_RATE, request.termMonths());

        String contractId = "PLN-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        String protocolId = "LOAN-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        String sagaId = "SAGA-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();

        log.info("Payroll loan requested | protocol={} enrollment={} amount={} term={}x",
            protocolId, request.enrollmentId(), request.requestedAmount(), request.termMonths());

        PayrollLoanContract contract = new PayrollLoanContract(
            contractId, protocolId, sagaId,
            request.customerAccount(), request.enrollmentId(), request.monthlyIncome(),
            request.requestedAmount(), MONTHLY_RATE, request.termMonths(), installmentAmount);
        register(contract);
        contract.addEvent("SAGA_STARTED", "Event published to Kafka — origination SAGA in progress");

        var event = PayrollLoanRequestedEvent.create(
            contractId, protocolId, sagaId,
            request.customerAccount(), request.enrollmentId(), request.monthlyIncome(),
            request.requestedAmount(), MONTHLY_RATE, request.termMonths(), installmentAmount);

        producer.publishLoanRequest(sagaId, event)
            .doOnError(e -> log.error("Failed to publish loan request to Kafka | protocol={}", protocolId, e))
            .subscribe();

        return PayrollLoanResponse.accepted(contractId, protocolId, installmentAmount);
    }

    /**
     * SAGA Step 1 — Payroll margin validation (read-only, idempotent).
     * Real payroll-lending rule: the installment must not consume more than
     * 35% of the informed monthly income.
     */
    public void validatePayrollMargin(BigDecimal monthlyIncome, BigDecimal installmentAmount) {
        BigDecimal maxMargin = monthlyIncome.multiply(MAX_PAYROLL_MARGIN).setScale(2, RoundingMode.HALF_UP);
        log.info("[SAGA-STEP-1] Validating payroll margin | income={} installment={} maxMargin={}",
            monthlyIncome, installmentAmount, maxMargin);
        if (installmentAmount.compareTo(maxMargin) > 0) {
            throw new PayrollMarginExceededException(
                "Installment $%s exceeds the 35%% payroll margin of the informed income (max. $%s)"
                    .formatted(installmentAmount, maxMargin));
        }
        log.info("[SAGA-STEP-1] ✓ Payroll margin OK | installment={} <= maxMargin={}", installmentAmount, maxMargin);
    }

    /**
     * SAGA Step 2 — Activates the contract (compensable checkpoint).
     * Equivalent to the transfer's "debit origin", but here the origin is the
     * bank's own capital: there is no customer account to debit.
     */
    public void activateContract(String contractId) {
        PayrollLoanContract c = getOrFail(contractId);
        c.status = ContractStatus.ACTIVE;
        c.addEvent("STEP2_CONTRACT_ACTIVATED", "Contract booked as a bank liability — ready for disbursement");
        log.info("[SAGA-STEP-2] ✓ Contract activated | contractId={}", contractId);
    }

    /**
     * SAGA Compensation — cancels the contract when Step 3 (customer credit) fails.
     */
    public void cancelContract(String contractId, String reason) {
        PayrollLoanContract c = getOrFail(contractId);
        c.status = ContractStatus.CANCELED;
        c.addEvent("CANCELED", reason);
        log.warn("[SAGA-COMP] Contract canceled | contractId={} reason={}", contractId, reason);
    }

    public void recordEvent(String contractId, String step, String message) {
        getOrFail(contractId).addEvent(step, message);
    }

    public List<Map<String, Object>> listContracts() {
        List<Map<String, Object>> result = new ArrayList<>();
        for (String id : order) {
            PayrollLoanContract c = contracts.get(id);
            if (c != null) result.add(c.toMap());
        }
        return result;
    }

    public Map<String, Object> getContractDto(String contractId) {
        return getOrFail(contractId).toMap();
    }

    /**
     * Simulates, on demand, one payroll deduction cycle for a specific
     * contract — handy for demos without waiting for the scheduler.
     */
    public Map<String, Object> simulateDeduction(String contractId) {
        PayrollLoanContract c = getOrFail(contractId);
        if (c.status != ContractStatus.ACTIVE) {
            throw new IllegalStateException(
                "Contract " + contractId + " is not active (current status: " + c.status + ")");
        }
        applyDeduction(c);
        return c.toMap();
    }

    /**
     * Simulates the monthly payroll/benefit deduction cycle for every active
     * contract. Compressed to a short interval (see application.yml, property
     * payroll-loan.deduction.interval) purely for demo purposes — in production
     * this would be driven by a real monthly job aligned with the payroll or
     * benefit payment date.
     *
     * Note: it never calls AccountService.debit — the amount is deducted BEFORE
     * it reaches the customer's checking account, so the balance held by
     * AccountService is untouched by this flow.
     */
    @Scheduled(fixedDelay = "${payroll-loan.deduction.interval:30s}")
    void processMonthlyDeductions() {
        for (String id : order) {
            PayrollLoanContract c = contracts.get(id);
            if (c != null && c.status == ContractStatus.ACTIVE) {
                applyDeduction(c);
            }
        }
    }

    private void applyDeduction(PayrollLoanContract c) {
        c.installmentsPaid++;
        // Annuity (fixed-installment) amortization: interest for the period accrues on the
        // outstanding balance BEFORE the installment is subtracted — that is why
        // the installment is fixed while its interest/principal split changes
        // month by month. Subtracting the installment straight from the balance
        // (without accruing interest) would pay the contract off too early.
        BigDecimal interest = c.outstandingBalance.multiply(c.monthlyRate).setScale(2, RoundingMode.HALF_UP);
        c.outstandingBalance = c.outstandingBalance.add(interest).subtract(c.installmentAmount).max(BigDecimal.ZERO);
        c.addEvent("PAYROLL_DEDUCTION", "Installment %d/%d deducted from the benefit (enrollment %s) — outstanding balance $%s"
            .formatted(c.installmentsPaid, c.termMonths, c.enrollmentId, c.outstandingBalance));
        log.info("[PAYROLL-DEDUCTION] contractId={} installment={}/{} outstandingBalance={}",
            c.contractId, c.installmentsPaid, c.termMonths, c.outstandingBalance);

        try {
            sqsPublisher.publish(Map.of(
                "contractId",         c.contractId,
                "type",               "PAYROLL_DEDUCTION",
                "enrollmentId",       c.enrollmentId,
                "installment",        c.installmentsPaid,
                "termMonths",         c.termMonths,
                "installmentAmount",  c.installmentAmount,
                "outstandingBalance", c.outstandingBalance,
                "message",            "Installment %d/%d of your payroll loan was deducted from your benefit"
                    .formatted(c.installmentsPaid, c.termMonths),
                "timestamp",          Instant.now().toString()
            ));
        } catch (Exception e) {
            log.warn("[PAYROLL-WARN] Failed to notify deduction via SQS (installment already processed) | contractId={}", c.contractId, e);
        }

        if (c.installmentsPaid >= c.termMonths || c.outstandingBalance.compareTo(BigDecimal.ZERO) <= 0) {
            c.status = ContractStatus.PAID_OFF;
            c.addEvent("PAID_OFF", "Payroll loan contract paid off after " + c.installmentsPaid + " installments");
            log.info("[PAYROLL-PAID-OFF] contractId={}", c.contractId);
        }
    }

    /**
     * Standard annuity formula: PMT = PV * i / (1 - (1+i)^-n)
     */
    private BigDecimal computeInstallment(BigDecimal requestedAmount, BigDecimal monthlyRate, int termMonths) {
        double pv = requestedAmount.doubleValue();
        double i = monthlyRate.doubleValue();
        double n = termMonths;
        double pmt = pv * i / (1 - Math.pow(1 + i, -n));
        return BigDecimal.valueOf(pmt).setScale(2, RoundingMode.HALF_UP);
    }

    private void register(PayrollLoanContract contract) {
        contracts.put(contract.contractId, contract);
        order.addFirst(contract.contractId);
        if (order.size() > MAX_HISTORY) {
            contracts.remove(order.pollLast());
        }
    }

    private PayrollLoanContract getOrFail(String contractId) {
        PayrollLoanContract c = contracts.get(contractId);
        if (c == null) {
            throw new ContractNotFoundException("Payroll loan contract not found: " + contractId);
        }
        return c;
    }

    // ── Inner class ──────────────────────────────────────────────────────────

    private static class PayrollLoanContract {
        final String contractId;
        final String protocolId;
        final String sagaId;
        final String customerAccount;
        final String enrollmentId;
        final BigDecimal monthlyIncome;
        final BigDecimal requestedAmount;
        final BigDecimal monthlyRate;
        final int termMonths;
        final BigDecimal installmentAmount;
        final String contractedAt = Instant.now().toString();

        volatile ContractStatus status = ContractStatus.AWAITING_DISBURSEMENT;
        volatile BigDecimal outstandingBalance;
        volatile int installmentsPaid = 0;
        final List<Map<String, Object>> history = new CopyOnWriteArrayList<>();

        PayrollLoanContract(String contractId, String protocolId, String sagaId,
                            String customerAccount, String enrollmentId, BigDecimal monthlyIncome,
                            BigDecimal requestedAmount, BigDecimal monthlyRate,
                            int termMonths, BigDecimal installmentAmount) {
            this.contractId = contractId;
            this.protocolId = protocolId;
            this.sagaId = sagaId;
            this.customerAccount = customerAccount;
            this.enrollmentId = enrollmentId;
            this.monthlyIncome = monthlyIncome;
            this.requestedAmount = requestedAmount;
            this.monthlyRate = monthlyRate;
            this.termMonths = termMonths;
            this.installmentAmount = installmentAmount;
            this.outstandingBalance = requestedAmount;
        }

        void addEvent(String step, String message) {
            Map<String, Object> e = new LinkedHashMap<>();
            e.put("step", step);
            e.put("message", message);
            e.put("timestamp", Instant.now().toString());
            history.add(e);
        }

        Map<String, Object> toMap() {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("contractId", contractId);
            m.put("protocolId", protocolId);
            m.put("sagaId", sagaId);
            m.put("customerAccount", customerAccount);
            m.put("enrollmentId", enrollmentId);
            m.put("monthlyIncome", monthlyIncome);
            m.put("requestedAmount", requestedAmount);
            m.put("monthlyRate", monthlyRate);
            m.put("termMonths", termMonths);
            m.put("installmentAmount", installmentAmount);
            m.put("outstandingBalance", outstandingBalance);
            m.put("installmentsPaid", installmentsPaid);
            m.put("status", status);
            m.put("contractedAt", contractedAt);
            m.put("history", history);
            return m;
        }
    }
}
