package com.cy.common.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Iou {

    private long order_id;

    private Double down_Payment;

    private Double order_amt;

    private Long update_time;

    public Iou(long l, Object down_payment, Object order_amt, Object update_time) {
    }
}
