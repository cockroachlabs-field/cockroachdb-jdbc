package io.cockroachdb.jdbc.demo;

import java.math.BigDecimal;

public class AccountLeg {
    private Long id;

    private BigDecimal amount;

    public AccountLeg(Long id, BigDecimal amount) {
        this.id = id;
        this.amount = amount;
    }

    public Long getId() {
        return id;
    }

    public BigDecimal getAmount() {
        return amount;
    }
}
