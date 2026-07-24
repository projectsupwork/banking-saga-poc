package com.bank.poc.transfer.domain.exception;

/**
 * Violation of a synchronous business rule in the transfer request.
 * Extends {@link IllegalArgumentException} so the web adapter can treat it
 * as a bad request (400) without knowing the concrete type.
 */
public class InvalidTransferException extends IllegalArgumentException {
    public InvalidTransferException(String message) {
        super(message);
    }
}
