package com.bank.poc.exception;

public class PayrollMarginExceededException extends RuntimeException {
    public PayrollMarginExceededException(String message) {
        super(message);
    }
}
