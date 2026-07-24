package com.bank.poc.dto;

import io.micronaut.serde.annotation.Serdeable;

import java.math.BigDecimal;
import java.time.Instant;

@Serdeable
public record PayrollLoanResponse(
    String contractId,
    String protocolId,
    String status,
    BigDecimal installmentAmount,
    String message,
    Instant timestamp
) {
    public static PayrollLoanResponse accepted(String contractId, String protocolId, BigDecimal installmentAmount) {
        return new PayrollLoanResponse(
            contractId,
            protocolId,
            "PROCESSING",
            installmentAmount,
            "Loan request received and being processed asynchronously",
            Instant.now()
        );
    }
}
