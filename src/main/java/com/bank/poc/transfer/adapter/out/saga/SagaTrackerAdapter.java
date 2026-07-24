package com.bank.poc.transfer.adapter.out.saga;

import com.bank.poc.transfer.domain.port.out.SagaTrackerPort;
import jakarta.inject.Singleton;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

/**
 * Outbound adapter that implements {@link SagaTrackerPort} in memory —
 * exclusive to the transfer domain (not shared with the payroll loan).
 * Feeds the SAGA history dashboard.
 */
@Singleton
public class SagaTrackerAdapter implements SagaTrackerPort {

    private static final int MAX = 50;

    private static final Map<String, String> HOLDERS = Map.of(
        "ACC-001", "Alice Johnson",
        "ACC-002", "Bob Smith",
        "ACC-003", "Carol Davis"
    );

    // Maintains insertion order (newest first)
    private final Deque<String> order = new ConcurrentLinkedDeque<>();
    private final Map<String, Record> records = new ConcurrentHashMap<>();

    @Override
    public void register(String protocolId, String sagaId,
                         String sourceAccount, String targetAccount, BigDecimal amount) {
        Record r = new Record(protocolId, sagaId, sourceAccount, targetAccount, amount);
        records.put(protocolId, r);
        order.addFirst(protocolId);
        if (order.size() > MAX) {
            records.remove(order.pollLast());
        }
    }

    @Override
    public void event(String protocolId, String step, String message) {
        Record r = records.get(protocolId);
        if (r != null) r.addEvent(step, message);
    }

    @Override
    public void complete(String protocolId) {
        Record r = records.get(protocolId);
        if (r != null) r.status = "COMPLETED";
    }

    @Override
    public void cancel(String protocolId, String reason) {
        Record r = records.get(protocolId);
        if (r != null) {
            r.status = "CANCELED";
            r.addEvent("CANCELED", reason);
        }
    }

    @Override
    public void fail(String protocolId, String reason) {
        Record r = records.get(protocolId);
        if (r != null) {
            r.status = "FAILED";
            r.addEvent("FAILED", reason);
        }
    }

    @Override
    public List<Map<String, Object>> listHistory() {
        List<Map<String, Object>> result = new ArrayList<>();
        for (String id : order) {
            Record r = records.get(id);
            if (r != null) result.add(r.toMap());
        }
        return result;
    }

    // ── Inner class ──────────────────────────────────────────────────────────

    private static class Record {
        final String protocolId;
        final String sagaId;
        final String sourceAccount;
        final String targetAccount;
        final BigDecimal amount;
        final String startedAt = Instant.now().toString();
        volatile String status = "PROCESSING";
        final List<Map<String, Object>> events = new CopyOnWriteArrayList<>();

        Record(String protocolId, String sagaId,
               String sourceAccount, String targetAccount, BigDecimal amount) {
            this.protocolId = protocolId;
            this.sagaId = sagaId;
            this.sourceAccount = sourceAccount;
            this.targetAccount = targetAccount;
            this.amount = amount;
        }

        void addEvent(String step, String message) {
            Map<String, Object> e = new LinkedHashMap<>();
            e.put("step", step);
            e.put("message", message);
            e.put("timestamp", Instant.now().toString());
            events.add(e);
        }

        Map<String, Object> toMap() {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("protocolId", protocolId);
            m.put("sagaId", sagaId);
            m.put("sourceAccount", sourceAccount);
            m.put("sourceHolder", HOLDERS.getOrDefault(sourceAccount, sourceAccount));
            m.put("targetAccount", targetAccount);
            m.put("targetHolder", HOLDERS.getOrDefault(targetAccount, targetAccount));
            m.put("amount", amount);
            m.put("status", status);
            m.put("startedAt", startedAt);
            m.put("events", events);
            return m;
        }
    }
}
