package com.cy.common.entity;

import lombok.Data;

import java.sql.Timestamp;

@Data
public class LoanOrder {

    private long order_id;

    private Double order_amt;

    private long update_time;
}
