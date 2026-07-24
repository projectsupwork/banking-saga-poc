package com.bank.poc.transfer.adapter.in.web.dto;

import io.micronaut.serde.annotation.Serdeable;
import jakarta.validation.constraints.*;
import java.math.BigDecimal;

/**
 * Payload received by POST /transfers.
 * Banking rules:
 *  - source != target
 *  - amount > 0 and at most $50,000 per transaction
 */
@Serdeable
public record TransferRequest(

    @NotBlank(message = "Source account is required")
    @Pattern(regexp = "ACC-\\d+", message = "Account must match format ACC-NUMBER")
    String sourceAccount,

    @NotBlank(message = "Target account is required")
    @Pattern(regexp = "ACC-\\d+", message = "Account must match format ACC-NUMBER")
    String targetAccount,

    @NotNull(message = "Amount is required")
    @DecimalMin(value = "0.01", message = "Minimum amount is $0.01")
    @DecimalMax(value = "50000.00", message = "Maximum amount per transaction is $50,000.00")
    BigDecimal amount
) {}
