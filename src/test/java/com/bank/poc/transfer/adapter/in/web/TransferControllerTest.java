package com.bank.poc.transfer.adapter.in.web;

import com.bank.poc.transfer.adapter.in.web.dto.TransferRequest;
import com.bank.poc.transfer.adapter.in.web.dto.TransferResponse;
import com.bank.poc.transfer.domain.Transfer;
import com.bank.poc.transfer.domain.port.in.RequestTransferUseCase;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.test.annotation.MockBean;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * Controller integration tests.
 * Uses Micronaut's real HTTP server + a mocked use case (inbound port).
 */
@MicronautTest
class TransferControllerTest {

    @Inject
    @Client("/")
    HttpClient client;

    @Inject
    RequestTransferUseCase requestTransfer;

    @MockBean(RequestTransferUseCase.class)
    RequestTransferUseCase mockRequestTransfer() {
        return Mockito.mock(RequestTransferUseCase.class);
    }

    @Test
    @DisplayName("POST /transfers with valid data returns 202 Accepted")
    void returns202ForValidTransfer() {
        var transfer = new Transfer("TRF-ABC123", "SAGA-ABC123", "ACC-001", "ACC-002", new BigDecimal("500.00"));
        when(requestTransfer.request(any(), any(), any())).thenReturn(transfer);

        var body = new TransferRequest("ACC-001", "ACC-002", new BigDecimal("500.00"));
        HttpResponse<TransferResponse> resp = client.toBlocking()
            .exchange(HttpRequest.POST("/transfers", body), TransferResponse.class);

        assertThat(resp.getStatus().getCode()).isEqualTo(HttpStatus.ACCEPTED.getCode());
        assertThat(resp.body().protocolId()).isEqualTo("TRF-ABC123");
        assertThat(resp.body().status()).isEqualTo("PROCESSING");
    }

    @Test
    @DisplayName("POST /transfers with source == target returns 400")
    void returns400ForSelfTransfer() {
        when(requestTransfer.request(any(), any(), any()))
            .thenThrow(new IllegalArgumentException("Invalid self-transfer"));

        var body = new TransferRequest("ACC-001", "ACC-001", new BigDecimal("500.00"));

        var resp = client.toBlocking().exchange(
            HttpRequest.POST("/transfers", body), String.class);

        assertThat(resp.getStatus().getCode()).isIn(400, 422);
    }

    @Test
    @DisplayName("POST /transfers with a negative amount returns 400")
    void returns400ForNegativeAmount() {
        var body = new TransferRequest("ACC-001", "ACC-002", new BigDecimal("-100.00"));

        var resp = client.toBlocking().exchange(
            HttpRequest.POST("/transfers", body), String.class);

        assertThat(resp.getStatus().getCode()).isIn(400, 422);
    }

    @Test
    @DisplayName("GET /transfers/health returns 200 with status UP")
    void healthCheckPing() {
        var resp = client.toBlocking().retrieve("/transfers/health");
        assertThat(resp).contains("UP");
    }
}
