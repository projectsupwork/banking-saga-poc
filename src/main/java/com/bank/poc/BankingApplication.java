package com.bank.poc;

import io.micronaut.runtime.Micronaut;
import io.micronaut.serde.annotation.SerdeImport;

@SerdeImport(Object.class)
public class BankingApplication {
    public static void main(String[] args) {
        Micronaut.run(BankingApplication.class, args);
    }
}
