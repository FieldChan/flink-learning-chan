package com.cy.common.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author: chenye
 * @Date: 2020/3/1 2:54
 * @Blog:
 * @Description:
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserPvEntity {
    public Long time;
    public String userId;
    public Long pvCount;
}
