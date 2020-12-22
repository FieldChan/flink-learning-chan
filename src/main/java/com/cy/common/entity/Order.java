package com.cy.common.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

/**
 * @time 2020-03-29 09:29:28
 * 订单的详细信息
 */
@Data
@Builder
@AllArgsConstructor
public class Order {
    /** 订单发生的时间 */
    long createTime;

    /** 订单发生的时间 */
    String orderCreateTime;

    /** 订单 id */
    String orderId;

    /** 用户id */
    String userId;

    /** 大小订单标记 */
    String orderTag;

    /** 商品id */
    int goodsId;

    /** 价格 */
    long price;

    /** 城市 */
    int cityId;
}
