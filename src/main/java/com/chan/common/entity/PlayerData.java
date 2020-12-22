package com.chan.common.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author: chenye
 * @Date: 2020/2/29 13:49
 * @Blog:
 * @Description:
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class PlayerData {
    /**
     * 赛季，球员，出场，首发，时间，助攻，抢断，盖帽，得分
     */
    public String season;
    public String player;
    public String play_num;
    public Integer first_court;
    public Double time;
    public Double assists;
    public Double steals;
    public Double blocks;
    public Double scores;

}
