package com.bank.poc.service;

import com.bank.poc.exception.InsufficientBalanceException;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for AccountService.
 * Covers every SAGA step and the compensation scenarios.
 */
@MicronautTest
class AccountServiceTest {

    @Inject
    AccountService accountService;

    @Test
    @DisplayName("Step 1: validating a sufficient balance does not throw")
    void validateSufficientBalance() {
        assertThatCode(() ->
            accountService.validateBalance("ACC-001", new BigDecimal("500.00"))
        ).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("Step 1: insufficient balance throws InsufficientBalanceException")
    void validateInsufficientBalance() {
        assertThatThrownBy(() ->
            accountService.validateBalance("ACC-001", new BigDecimal("99999.00"))
        ).isInstanceOf(InsufficientBalanceException.class)
         .hasMessageContaining("Insufficient balance");
    }

    @Test
    @DisplayName("Step 2: debit reduces the source account balance")
    void debitReducesBalance() {
        BigDecimal initialBalance = accountService.getBalance("ACC-001");
        BigDecimal newBalance = accountService.debit("ACC-001", new BigDecimal("100.00"));

        assertThat(newBalance).isEqualByComparingTo(initialBalance.subtract(new BigDecimal("100.00")));
    }

    @Test
    @DisplayName("Step 3: credit increases the target account balance")
    void creditIncreasesBalance() {
        BigDecimal initialBalance = accountService.getBalance("ACC-002");
        BigDecimal newBalance = accountService.credit("ACC-002", new BigDecimal("200.00"));

        assertThat(newBalance).isEqualByComparingTo(initialBalance.add(new BigDecimal("200.00")));
    }

    @Test
    @DisplayName("Compensation: reverting a debit restores the original balance")
    void compensationRevertsDebit() {
        BigDecimal originalBalance = accountService.getBalance("ACC-001");
        BigDecimal amount = new BigDecimal("300.00");

        // Simulates Step 2 (debit)
        accountService.debit("ACC-001", amount);

        // Simulates compensation (revert debit when Step 3 fails)
        BigDecimal balanceAfterCompensation = accountService.creditCompensation("ACC-001", amount);

        assertThat(balanceAfterCompensation).isEqualByComparingTo(originalBalance);
    }

    @Test
    @DisplayName("Unknown account throws AccountNotFoundException")
    void unknownAccountThrows() {
        assertThatThrownBy(() ->
            accountService.validateBalance("ACC-999", BigDecimal.ONE)
        ).hasMessageContaining("ACC-999");
    }

    @Test
    @DisplayName("Concurrent operations on the same account are atomic")
    void concurrentOperationsAreAtomic() throws InterruptedException {
        BigDecimal initialBalance = accountService.getBalance("ACC-002");
        BigDecimal amountPerThread = new BigDecimal("10.00");
        int threads = 5;

        Thread[] ts = new Thread[threads];
        for (int i = 0; i < threads; i++) {
            ts[i] = new Thread(() -> accountService.credit("ACC-002", amountPerThread));
            ts[i].start();
        }
        for (Thread t : ts) t.join();

        BigDecimal expectedBalance = initialBalance.add(amountPerThread.multiply(new BigDecimal(threads)));
        assertThat(accountService.getBalance("ACC-002")).isEqualByComparingTo(expectedBalance);
    }
}
