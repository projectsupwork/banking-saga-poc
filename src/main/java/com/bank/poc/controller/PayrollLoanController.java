package com.bank.poc.controller;

import com.bank.poc.dto.PayrollLoanRequest;
import com.bank.poc.dto.PayrollLoanResponse;
import com.bank.poc.exception.ContractNotFoundException;
import com.bank.poc.service.PayrollLoanService;
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
 * REST endpoint of the Payroll Loan product.
 *
 * Contract:
 *   POST /loans/payroll
 *   Body: { "customerAccount": "ACC-001", "enrollmentId": "ENR-123", "monthlyIncome": 3000.00,
 *           "requestedAmount": 5000.00, "termMonths": 36 }
 *   → 202 Accepted + { "contractId": "PLN-XXXX", "protocolId": "LOAN-XXXX", "installmentAmount": ... }
 *
 *   GET  /loans/payroll                            → list contracts
 *   GET  /loans/payroll/{contractId}               → detail + SAGA history
 *   POST /loans/payroll/{contractId}/simulate-deduction → manually triggers one
 *        payroll deduction cycle (demo purposes — the real cycle also runs on
 *        its own, see payroll-loan.deduction.interval in application.yml)
 */
@Validated
@Controller("/loans/payroll")
public class PayrollLoanController {

    private static final Logger log = LoggerFactory.getLogger(PayrollLoanController.class);

    @Inject
    private PayrollLoanService service;

    @Post
    @Status(HttpStatus.ACCEPTED)
    public HttpResponse<PayrollLoanResponse> requestLoan(@Valid @Body PayrollLoanRequest request) {
        log.info("POST /loans/payroll | account={} enrollment={} amount={} term={}x",
            request.customerAccount(), request.enrollmentId(), request.requestedAmount(), request.termMonths());

        try {
            PayrollLoanResponse response = service.startOrigination(request);
            return HttpResponse.accepted().body(response);
        } catch (IllegalArgumentException e) {
            log.warn("Invalid request: {}", e.getMessage());
            return HttpResponse.badRequest();
        }
    }

    @Get
    public HttpResponse<List<Map<String, Object>>> list() {
        return HttpResponse.ok(service.listContracts());
    }

    @Get("/{contractId}")
    public HttpResponse<Map<String, Object>> detail(String contractId) {
        try {
            return HttpResponse.ok(service.getContractDto(contractId));
        } catch (ContractNotFoundException e) {
            return HttpResponse.notFound();
        }
    }

    @Post("/{contractId}/simulate-deduction")
    public HttpResponse<Map<String, Object>> simulateDeduction(String contractId) {
        try {
            return HttpResponse.ok(service.simulateDeduction(contractId));
        } catch (ContractNotFoundException e) {
            return HttpResponse.notFound();
        } catch (IllegalStateException e) {
            log.warn("Deduction rejected: {}", e.getMessage());
            return HttpResponse.status(HttpStatus.CONFLICT);
        }
    }
}
