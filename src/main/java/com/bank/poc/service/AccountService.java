package com.bank.poc.service;

import com.bank.poc.exception.AccountNotFoundException;
import com.bank.poc.exception.InsufficientBalanceException;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * In-memory "database".
 *
 * Practical analogy: think of a Map&lt;AccountId, Balance&gt; as a SQL table with
 * per-account row locks — isolation via ReadWriteLock guarantees that two
 * transfers hitting the same account never race.
 *
 * Pre-loaded demo accounts:
 *   ACC-001  Alice Johnson  $5,000.00
 *   ACC-002  Bob Smith      $1,000.00
 *   ACC-003  Carol Davis    $  250.00
 */
@Singleton
public class AccountService {

    private static final Logger log = LoggerFactory.getLogger(AccountService.class);

    private static final Map<String, String> HOLDERS = Map.of(
        "ACC-001", "Alice Johnson",
        "ACC-002", "Bob Smith",
        "ACC-003", "Carol Davis"
    );

    // Balance per account — simulating an ACCOUNTS table
    private final Map<String, BigDecimal> balances = new ConcurrentHashMap<>(Map.of(
        "ACC-001", new BigDecimal("5000.00"),
        "ACC-002", new BigDecimal("1000.00"),
        "ACC-003", new BigDecimal("250.00")
    ));

    // Per-account lock for atomic operations
    private final Map<String, ReentrantReadWriteLock> locks = new ConcurrentHashMap<>(Map.of(
        "ACC-001", new ReentrantReadWriteLock(),
        "ACC-002", new ReentrantReadWriteLock(),
        "ACC-003", new ReentrantReadWriteLock()
    ));

    /**
     * SAGA Step 1 — Validation without state changes.
     * Idempotent: safe to call multiple times.
     */
    public void validateBalance(String accountId, BigDecimal amount) {
        log.info("[SAGA-STEP-1] Validating balance | account={} amount={}", accountId, amount);

        BigDecimal balance = getBalance(accountId);
        if (balance.compareTo(amount) < 0) {
            throw new InsufficientBalanceException(
                "Insufficient balance in account %s: available=%s requested=%s"
                    .formatted(accountId, balance, amount));
        }
        log.info("[SAGA-STEP-1] ✓ Balance ok | account={} available={}", accountId, balance);
    }

    /**
     * SAGA Step 2 — Debit with checkpoint.
     * Compensable transaction: reversed by creditCompensation() if step 3 fails.
     */
    public BigDecimal debit(String accountId, BigDecimal amount) {
        log.info("[SAGA-STEP-2] Debiting | account={} amount={}", accountId, amount);
        return atomicOperation(accountId, amount.negate());
    }

    /**
     * SAGA Step 3 — Credit.
     */
    public BigDecimal credit(String accountId, BigDecimal amount) {
        log.info("[SAGA-STEP-3] Crediting | account={} amount={}", accountId, amount);
        return atomicOperation(accountId, amount);
    }

    /**
     * SAGA Compensation (revert Step 2) — restores the debited balance.
     * Called when Step 3 (credit) fails.
     */
    public BigDecimal creditCompensation(String accountId, BigDecimal amount) {
        log.warn("[SAGA-COMP] Reverting debit | account={} amount={}", accountId, amount);
        return atomicOperation(accountId, amount); // credit reverses the debit
    }

    public List<Map<String, Object>> listAccounts() {
        return balances.entrySet().stream()
            .sorted(Map.Entry.comparingByKey())
            .map(e -> {
                Map<String, Object> m = new java.util.LinkedHashMap<>();
                m.put("id", e.getKey());
                m.put("holder", HOLDERS.getOrDefault(e.getKey(), "Unknown"));
                m.put("balance", e.getValue());
                return m;
            })
            .collect(java.util.stream.Collectors.toList());
    }

    public BigDecimal getBalance(String accountId) {
        BigDecimal balance = balances.get(accountId);
        if (balance == null) {
            throw new AccountNotFoundException("Account not found: " + accountId);
        }
        return balance;
    }

    private BigDecimal atomicOperation(String accountId, BigDecimal delta) {
        ReentrantReadWriteLock lock = locks.computeIfAbsent(accountId, k -> new ReentrantReadWriteLock());
        lock.writeLock().lock();
        try {
            BigDecimal current = getBalance(accountId);
            BigDecimal updated = current.add(delta);
            if (updated.compareTo(BigDecimal.ZERO) < 0) {
                throw new InsufficientBalanceException(
                    "Insufficient balance after operation: " + accountId);
            }
            balances.put(accountId, updated);
            log.debug("Balance updated | account={} {} → {}", accountId, current, updated);
            return updated;
        } finally {
            lock.writeLock().unlock();
        }
    }
}
