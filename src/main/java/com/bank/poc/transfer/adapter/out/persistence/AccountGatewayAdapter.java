package com.bank.poc.transfer.adapter.out.persistence;

import com.bank.poc.service.AccountService;
import com.bank.poc.transfer.domain.port.out.AccountGatewayPort;
import jakarta.inject.Singleton;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/**
 * Outbound adapter that implements {@link AccountGatewayPort} by delegating
 * to the shared {@link AccountService} (also used by the payroll loan
 * domain). The transfer domain never sees AccountService directly — only
 * the port.
 */
@Singleton
public class AccountGatewayAdapter implements AccountGatewayPort {

    private final AccountService accountService;

    public AccountGatewayAdapter(AccountService accountService) {
        this.accountService = accountService;
    }

    @Override
    public void validateBalance(String accountId, BigDecimal amount) {
        accountService.validateBalance(accountId, amount);
    }

    @Override
    public BigDecimal debit(String accountId, BigDecimal amount) {
        return accountService.debit(accountId, amount);
    }

    @Override
    public BigDecimal credit(String accountId, BigDecimal amount) {
        return accountService.credit(accountId, amount);
    }

    @Override
    public BigDecimal creditCompensation(String accountId, BigDecimal amount) {
        return accountService.creditCompensation(accountId, amount);
    }

    @Override
    public BigDecimal getBalance(String accountId) {
        return accountService.getBalance(accountId);
    }

    @Override
    public List<Map<String, Object>> listAccounts() {
        return accountService.listAccounts();
    }
}
