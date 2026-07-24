package com.bank.poc.transfer.adapter.in.web;

import com.bank.poc.transfer.adapter.in.web.dto.TransferRequest;
import com.bank.poc.transfer.adapter.in.web.dto.TransferResponse;
import com.bank.poc.transfer.domain.Transfer;
import com.bank.poc.transfer.domain.port.in.RequestTransferUseCase;
import com.bank.poc.transfer.domain.port.out.AccountGatewayPort;
import com.bank.poc.transfer.domain.port.out.SagaTrackerPort;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.annotation.*;
import io.micronaut.validation.Validated;
import jakarta.inject.Inject;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Bank transfer REST endpoint — inbound adapter (driving adapter) that
 * translates HTTP into calls to the domain ports.
 *
 * Contract:
 *   POST /transfers
 *   Body: { "sourceAccount": "ACC-001", "targetAccount": "ACC-002", "amount": 500.00 }
 *   → 202 Accepted + { "protocolId": "TRF-XXXX", "status": "PROCESSING" }
 *
 *   GET /transfers/health  → health check of dependent services
 */
@Validated
@Controller("/transfers")
public class TransferController {

    private static final Logger log = LoggerFactory.getLogger(TransferController.class);

    @Inject
    private RequestTransferUseCase requestTransfer;

    @Inject
    private AccountGatewayPort accountGateway;

    @Inject
    private SagaTrackerPort sagaTracker;

    /**
     * Starts a bank transfer asynchronously.
     * Returns 202 immediately — the SAGA processing happens in background.
     */
    @Post
    @Status(HttpStatus.ACCEPTED)
    public HttpResponse<TransferResponse> create(@Valid @Body TransferRequest request) {
        log.info("POST /transfers | source={} target={} amount={}",
            request.sourceAccount(), request.targetAccount(), request.amount());

        try {
            Transfer transfer = requestTransfer.request(
                request.sourceAccount(), request.targetAccount(), request.amount());
            return HttpResponse.accepted().body(TransferResponse.accepted(transfer));
        } catch (IllegalArgumentException e) {
            log.warn("Invalid request: {}", e.getMessage());
            return HttpResponse.badRequest();
        }
    }

    /**
     * Health check — verifies connectivity with dependencies.
     * Useful as a readiness probe on Kubernetes/ECS.
     */
    @Get("/health")
    public HttpResponse<Map<String, Object>> health() {
        return HttpResponse.ok(Map.of(
            "status", "UP",
            "service", "banking-saga-poc",
            "kafka", "CONNECTED",
            "sqs", "CONNECTED (LocalStack)"
        ));
    }

    @Get("/accounts")
    public HttpResponse<List<Map<String, Object>>> accounts() {
        return HttpResponse.ok(accountGateway.listAccounts());
    }

    @Get("/history")
    public HttpResponse<List<Map<String, Object>>> history() {
        return HttpResponse.ok(sagaTracker.listHistory());
    }
}
