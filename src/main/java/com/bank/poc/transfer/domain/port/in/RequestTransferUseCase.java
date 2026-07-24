package com.bank.poc.transfer.domain.port.in;

import com.bank.poc.transfer.domain.Transfer;

import java.math.BigDecimal;

/**
 * Inbound port (driving port) — starts a bank transfer.
 * Implemented by the application layer, consumed by the web adapter.
 */
public interface RequestTransferUseCase {
    Transfer request(String sourceAccount, String targetAccount, BigDecimal amount);
}
