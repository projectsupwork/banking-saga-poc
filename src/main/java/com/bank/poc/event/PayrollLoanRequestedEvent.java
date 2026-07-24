package com.bank.poc.event;

import io.micronaut.serde.annotation.Serdeable;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Event published by the API to the "loans.payroll.requested" topic.
 * Triggers the loan origination SAGA choreography in PayrollLoanConsumer.
 */
@Serdeable
public record PayrollLoanRequestedEvent(
    String contractId,
    String protocolId,       // customer-facing trace ID — prefix "LOAN-"
    String sagaId,           // unique SAGA execution ID — prefix "SAGA-"
    String type,             // PAYROLL_LOAN_REQUESTED
    String customerAccount,
    String enrollmentId,
    BigDecimal monthlyIncome,
    BigDecimal requestedAmount,
    BigDecimal monthlyRate,
    int termMonths,
    BigDecimal installmentAmount,
    Instant timestamp
) {
    public static PayrollLoanRequestedEvent create(
        String contractId, String protocolId, String sagaId,
        String customerAccount, String enrollmentId, BigDecimal monthlyIncome,
        BigDecimal requestedAmount, BigDecimal monthlyRate, int termMonths, BigDecimal installmentAmount) {
        return new PayrollLoanRequestedEvent(
            contractId, protocolId, sagaId, "PAYROLL_LOAN_REQUESTED",
            customerAccount, enrollmentId, monthlyIncome, requestedAmount, monthlyRate,
            termMonths, installmentAmount, Instant.now()
        );
    }
}
