package com.bank.poc.controller;

import com.bank.poc.dto.PayrollLoanRequest;
import com.bank.poc.dto.PayrollLoanResponse;
import com.bank.poc.exception.ContractNotFoundException;
import com.bank.poc.service.PayrollLoanService;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.test.annotation.MockBean;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.math.BigDecimal;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * Integration tests for PayrollLoanController.
 * Uses Micronaut's real HTTP server + a mocked Service (same pattern as TransferControllerTest).
 */
@MicronautTest
class PayrollLoanControllerTest {

    @Inject
    @Client("/")
    HttpClient client;

    @Inject
    PayrollLoanService service;

    @MockBean(PayrollLoanService.class)
    PayrollLoanService mockService() {
        return Mockito.mock(PayrollLoanService.class);
    }

    @Test
    @DisplayName("POST /loans/payroll with valid data returns 202 Accepted")
    void returns202ForValidOrigination() {
        var response = PayrollLoanResponse.accepted("PLN-ABC123", "LOAN-ABC123", new BigDecimal("150.00"));
        when(service.startOrigination(any())).thenReturn(response);

        var body = new PayrollLoanRequest("ACC-001", "ENR-9001",
            new BigDecimal("3000.00"), new BigDecimal("5000.00"), 36);
        HttpResponse<PayrollLoanResponse> resp = client.toBlocking()
            .exchange(HttpRequest.POST("/loans/payroll", body), PayrollLoanResponse.class);

        assertThat(resp.getStatus().getCode()).isEqualTo(HttpStatus.ACCEPTED.getCode());
        assertThat(resp.body().contractId()).isEqualTo("PLN-ABC123");
        assertThat(resp.body().status()).isEqualTo("PROCESSING");
    }

    @Test
    @DisplayName("POST /loans/payroll with a duplicate enrollment returns 400")
    void returns400ForDuplicateEnrollment() {
        when(service.startOrigination(any()))
            .thenThrow(new IllegalArgumentException("Enrollment already has an open payroll loan contract"));

        var body = new PayrollLoanRequest("ACC-001", "ENR-9002",
            new BigDecimal("3000.00"), new BigDecimal("5000.00"), 36);

        assertThatThrownBy(() ->
            client.toBlocking().exchange(HttpRequest.POST("/loans/payroll", body), String.class)
        ).isInstanceOf(HttpClientResponseException.class)
         .satisfies(e -> assertThat(((HttpClientResponseException) e).getStatus().getCode()).isIn(400, 422));
    }

    @Test
    @DisplayName("POST /loans/payroll with a term outside the 24-120 range returns 400")
    void returns400ForInvalidTerm() {
        var body = new PayrollLoanRequest("ACC-001", "ENR-9003",
            new BigDecimal("3000.00"), new BigDecimal("5000.00"), 12);

        assertThatThrownBy(() ->
            client.toBlocking().exchange(HttpRequest.POST("/loans/payroll", body), String.class)
        ).isInstanceOf(HttpClientResponseException.class)
         .satisfies(e -> assertThat(((HttpClientResponseException) e).getStatus().getCode()).isIn(400, 422));
    }

    @Test
    @DisplayName("GET /loans/payroll/{id} returns the contract detail")
    void returnsContractDetail() {
        Map<String, Object> contract = Map.of("contractId", "PLN-ABC123", "status", "ACTIVE");
        when(service.getContractDto("PLN-ABC123")).thenReturn(contract);

        var resp = client.toBlocking().retrieve("/loans/payroll/PLN-ABC123");
        assertThat(resp).contains("PLN-ABC123");
    }

    @Test
    @DisplayName("GET /loans/payroll/{id} for an unknown contract returns 404")
    void returns404ForUnknownContract() {
        when(service.getContractDto("PLN-MISSING"))
            .thenThrow(new ContractNotFoundException("Payroll loan contract not found: PLN-MISSING"));

        assertThatThrownBy(() ->
            client.toBlocking().exchange(HttpRequest.GET("/loans/payroll/PLN-MISSING"), String.class)
        ).isInstanceOf(HttpClientResponseException.class)
         .satisfies(e -> assertThat(((HttpClientResponseException) e).getStatus().getCode()).isEqualTo(404));
    }

    @Test
    @DisplayName("POST /loans/payroll/{id}/simulate-deduction on a non-active contract returns 409")
    void returns409ForDeductionOnNonActiveContract() {
        when(service.simulateDeduction("PLN-ABC123"))
            .thenThrow(new IllegalStateException("Contract PLN-ABC123 is not active"));

        assertThatThrownBy(() ->
            client.toBlocking().exchange(
                HttpRequest.POST("/loans/payroll/PLN-ABC123/simulate-deduction", null), String.class)
        ).isInstanceOf(HttpClientResponseException.class)
         .satisfies(e -> assertThat(((HttpClientResponseException) e).getStatus().getCode()).isEqualTo(409));
    }

    @Test
    @DisplayName("GET /loans/payroll lists contracts")
    void listsContracts() {
        when(service.listContracts()).thenReturn(java.util.List.of(
            Map.of("contractId", "PLN-ABC123", "status", "ACTIVE")
        ));

        var resp = client.toBlocking().retrieve("/loans/payroll");
        assertThat(resp).contains("PLN-ABC123");
    }
}
