package com.bank.poc.dto;

import io.micronaut.serde.annotation.Serdeable;
import jakarta.validation.constraints.*;

import java.math.BigDecimal;

/**
 * Payload received by POST /loans/payroll.
 * Banking rules:
 *  - term between 24 and 120 months (mirrors real-world payroll loans)
 *  - the resulting installment must not exceed 35% of the informed monthly
 *    income (payroll margin) — validated in SAGA Step 1, not here
 */
@Serdeable
public record PayrollLoanRequest(

    @NotBlank(message = "Customer account is required")
    @Pattern(regexp = "ACC-\\d+", message = "Account must match format ACC-NUMBER")
    String customerAccount,

    @NotBlank(message = "Payroll/benefit enrollment id is required")
    String enrollmentId,

    @NotNull(message = "Monthly income is required to compute the payroll margin")
    @DecimalMin(value = "0.01", message = "Monthly income must be positive")
    BigDecimal monthlyIncome,

    @NotNull(message = "Requested amount is required")
    @DecimalMin(value = "100.00", message = "Minimum amount is $100.00")
    @DecimalMax(value = "100000.00", message = "Maximum amount per contract is $100,000.00")
    BigDecimal requestedAmount,

    @NotNull(message = "Term is required")
    @Min(value = 24, message = "Minimum term is 24 months")
    @Max(value = 120, message = "Maximum term is 120 months")
    Integer termMonths
) {}
