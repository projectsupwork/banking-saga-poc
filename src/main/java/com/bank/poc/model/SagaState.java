package com.bank.poc.model;

/**
 * State machine of the transfer SAGA.
 *
 * Happy path:   STARTED → VALIDATING → DEBIT_COMPLETED → CREDIT_COMPLETED → COMPLETED
 * Compensation: COMPLETED ← CREDIT_COMPLETED ← DEBIT_REVERTED ← COMPENSATING ← [error]
 */
public enum SagaState {
    STARTED,
    VALIDATING,
    DEBIT_COMPLETED,
    CREDIT_COMPLETED,
    COMPLETED,
    COMPENSATING,
    DEBIT_REVERTED,
    CANCELED
}
