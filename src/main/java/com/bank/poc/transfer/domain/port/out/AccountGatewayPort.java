package com.bank.poc.transfer.domain.port.out;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/**
 * Outbound port (driven port) — access to checking-account state.
 * Abstracts the real source of the balance (today: in-memory
 * {@code AccountService}, shared with the payroll loan domain).
 */
public interface AccountGatewayPort {

    void validateBalance(String accountId, BigDecimal amount);

    BigDecimal debit(String accountId, BigDecimal amount);

    BigDecimal credit(String accountId, BigDecimal amount);

    BigDecimal creditCompensation(String accountId, BigDecimal amount);

    BigDecimal getBalance(String accountId);

    List<Map<String, Object>> listAccounts();
}
