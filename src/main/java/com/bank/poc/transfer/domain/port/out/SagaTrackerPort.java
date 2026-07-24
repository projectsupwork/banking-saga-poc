package com.bank.poc.transfer.domain.port.out;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/**
 * Outbound port (driven port) — audit/observability of the transfer SAGA
 * steps, consumed by the history dashboard.
 */
public interface SagaTrackerPort {

    void register(String protocolId, String sagaId, String sourceAccount, String targetAccount, BigDecimal amount);

    void event(String protocolId, String step, String message);

    void complete(String protocolId);

    void cancel(String protocolId, String reason);

    void fail(String protocolId, String reason);

    List<Map<String, Object>> listHistory();
}
