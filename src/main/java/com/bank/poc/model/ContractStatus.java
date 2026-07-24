package com.bank.poc.model;

/**
 * State machine of the payroll loan contract.
 *
 * Happy path:   AWAITING_DISBURSEMENT → ACTIVE → (monthly deductions) → PAID_OFF
 * Compensation: AWAITING_DISBURSEMENT → CANCELED  (Step 3 failed to credit the customer)
 */
public enum ContractStatus {
    AWAITING_DISBURSEMENT,
    ACTIVE,
    PAID_OFF,
    CANCELED
}
