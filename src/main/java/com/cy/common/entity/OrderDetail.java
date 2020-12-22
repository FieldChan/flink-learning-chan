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
public class OrderDetail {

    /** 订单 id */
    String orderId;

    /** 城市 */
    int cityId;

    /** 城市名称 */
    String cityName;

    /** 商品id */
    int goodsId;

    /** 用户id */
    String userId;

    /** 价格 */
    int price;

    /** 订单发生的时间 */
    int createTime;




}
