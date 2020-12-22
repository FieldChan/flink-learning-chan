package com.chan.common.entity;

import lombok.Data;

@Data
public class LoanOrder {

    private long order_id;

    private Double order_amt;

    private long update_time;
}
