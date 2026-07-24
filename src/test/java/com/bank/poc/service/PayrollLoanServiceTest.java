package com.bank.poc.service;

import com.bank.poc.dto.PayrollLoanRequest;
import com.bank.poc.dto.PayrollLoanResponse;
import com.bank.poc.exception.ContractNotFoundException;
import com.bank.poc.exception.PayrollMarginExceededException;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for PayrollLoanService.
 * Covers the annuity formula, the payroll margin (Step 1), the contract life
 * cycle (Steps 2/3 and compensation) and the payroll deduction simulation.
 *
 * Each test uses its own enrollment id so it never collides with the
 * "enrollment already has an open contract" rule (same caution taken in
 * AccountServiceTest for accounts shared across tests).
 */
@MicronautTest
class PayrollLoanServiceTest {

    @Inject
    PayrollLoanService service;

    @Test
    @DisplayName("Origination within the payroll margin is accepted and prices the installment with the annuity formula")
    void originationWithinMarginIsAccepted() {
        var request = new PayrollLoanRequest("ACC-001", "ENR-1001",
            new BigDecimal("3000.00"), new BigDecimal("5000.00"), 36);

        PayrollLoanResponse response = service.startOrigination(request);

        assertThat(response.contractId()).startsWith("PLN-");
        assertThat(response.protocolId()).startsWith("LOAN-");
        assertThat(response.status()).isEqualTo("PROCESSING");
        assertThat(response.installmentAmount()).isPositive();

        // Annuity-formula consistency: PV = PMT * (1 - (1+i)^-n) / i
        double i = 0.0154;
        double n = 36;
        double expectedPv = response.installmentAmount().doubleValue() * (1 - Math.pow(1 + i, -n)) / i;
        assertThat(expectedPv).isCloseTo(5000.00, within(1.0));
    }

    @Test
    @DisplayName("Step 1: installment within 35% of the income does not throw")
    void payrollMarginWithinLimitDoesNotThrow() {
        assertThatCode(() ->
            service.validatePayrollMargin(new BigDecimal("3000.00"), new BigDecimal("1000.00"))
        ).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("Step 1: installment above 35% of the income throws PayrollMarginExceededException")
    void payrollMarginExceededThrows() {
        assertThatThrownBy(() ->
            service.validatePayrollMargin(new BigDecimal("1000.00"), new BigDecimal("500.00"))
        ).isInstanceOf(PayrollMarginExceededException.class)
         .hasMessageContaining("payroll margin");
    }

    @Test
    @DisplayName("Origination whose amount/term exceeds the margin is rejected by Step 1 (via SAGA)")
    void originationAboveMarginIsDetectableInStep1() {
        // Low income + short term forces an installment high enough to violate the margin
        var request = new PayrollLoanRequest("ACC-002", "ENR-1002",
            new BigDecimal("500.00"), new BigDecimal("10000.00"), 24);

        PayrollLoanResponse response = service.startOrigination(request);

        // The margin validation runs asynchronously in the consumer (Step 1);
        // here we assert the same rule, invoked directly, would reject this installment.
        assertThatThrownBy(() ->
            service.validatePayrollMargin(request.monthlyIncome(), response.installmentAmount())
        ).isInstanceOf(PayrollMarginExceededException.class);
    }

    @Test
    @DisplayName("A second origination for an enrollment with an open contract is rejected")
    void enrollmentWithOpenContractCannotOriginateAgain() {
        var request = new PayrollLoanRequest("ACC-001", "ENR-1003",
            new BigDecimal("3000.00"), new BigDecimal("2000.00"), 24);
        service.startOrigination(request);

        assertThatThrownBy(() -> service.startOrigination(request))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("ENR-1003");
    }

    @Test
    @DisplayName("Step 2 → Step 3: activating the contract switches the status to ACTIVE")
    void activateContractSwitchesStatusToActive() {
        var request = new PayrollLoanRequest("ACC-001", "ENR-1004",
            new BigDecimal("3000.00"), new BigDecimal("2000.00"), 24);
        PayrollLoanResponse response = service.startOrigination(request);

        service.activateContract(response.contractId());

        Map<String, Object> contract = service.getContractDto(response.contractId());
        assertThat(contract.get("status").toString()).isEqualTo("ACTIVE");
    }

    @Test
    @DisplayName("Compensation: canceling the contract switches the status to CANCELED")
    void cancelContractSwitchesStatusToCanceled() {
        var request = new PayrollLoanRequest("ACC-001", "ENR-1005",
            new BigDecimal("3000.00"), new BigDecimal("2000.00"), 24);
        PayrollLoanResponse response = service.startOrigination(request);

        service.cancelContract(response.contractId(), "Credit failed — target account not found");

        Map<String, Object> contract = service.getContractDto(response.contractId());
        assertThat(contract.get("status").toString()).isEqualTo("CANCELED");
    }

    @Test
    @DisplayName("Unknown contract throws ContractNotFoundException")
    void unknownContractThrows() {
        assertThatThrownBy(() -> service.getContractDto("PLN-DOES-NOT-EXIST"))
            .isInstanceOf(ContractNotFoundException.class)
            .hasMessageContaining("PLN-DOES-NOT-EXIST");
    }

    @Test
    @DisplayName("Simulating a deduction on a non-active contract throws IllegalStateException")
    void simulateDeductionOnNonActiveContractThrows() {
        var request = new PayrollLoanRequest("ACC-001", "ENR-1006",
            new BigDecimal("3000.00"), new BigDecimal("2000.00"), 24);
        PayrollLoanResponse response = service.startOrigination(request);

        assertThatThrownBy(() -> service.simulateDeduction(response.contractId()))
            .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("Simulating a deduction lowers the outstanding balance and bumps installments paid, without touching AccountService")
    void simulateDeductionLowersOutstandingBalance() {
        var request = new PayrollLoanRequest("ACC-001", "ENR-1007",
            new BigDecimal("3000.00"), new BigDecimal("2000.00"), 24);
        PayrollLoanResponse response = service.startOrigination(request);
        service.activateContract(response.contractId());

        Map<String, Object> before = service.getContractDto(response.contractId());
        BigDecimal balanceBefore = new BigDecimal(before.get("outstandingBalance").toString());

        Map<String, Object> after = service.simulateDeduction(response.contractId());

        assertThat(after.get("installmentsPaid")).isEqualTo(1);
        BigDecimal balanceAfter = new BigDecimal(after.get("outstandingBalance").toString());

        // Annuity amortization: the outstanding balance accrues the period's
        // interest before the installment is subtracted (fixed rate of
        // 1.54%/month — same as the service).
        BigDecimal monthlyRate = new BigDecimal("0.0154");
        BigDecimal interest = balanceBefore.multiply(monthlyRate).setScale(2, java.math.RoundingMode.HALF_UP);
        BigDecimal expectedBalance = balanceBefore.add(interest).subtract(response.installmentAmount());
        assertThat(balanceAfter).isEqualByComparingTo(expectedBalance);
    }

    @Test
    @DisplayName("Contract is paid off after every installment of the term is paid")
    void contractIsPaidOffAfterLastInstallment() {
        int termMonths = 24;
        var request = new PayrollLoanRequest("ACC-001", "ENR-1008",
            new BigDecimal("3000.00"), new BigDecimal("2000.00"), termMonths);
        PayrollLoanResponse response = service.startOrigination(request);
        service.activateContract(response.contractId());

        Map<String, Object> last = null;
        for (int i = 0; i < termMonths; i++) {
            last = service.simulateDeduction(response.contractId());
        }

        assertThat(last.get("status").toString()).isEqualTo("PAID_OFF");
        assertThat(last.get("installmentsPaid")).isEqualTo(termMonths);
    }

    @Test
    @DisplayName("Contracts show up in the listing")
    void contractShowsUpInListing() {
        var request = new PayrollLoanRequest("ACC-001", "ENR-1009",
            new BigDecimal("3000.00"), new BigDecimal("2000.00"), 24);
        PayrollLoanResponse response = service.startOrigination(request);

        List<Map<String, Object>> contracts = service.listContracts();

        assertThat(contracts)
            .extracting(m -> m.get("contractId"))
            .contains(response.contractId());
    }
}
