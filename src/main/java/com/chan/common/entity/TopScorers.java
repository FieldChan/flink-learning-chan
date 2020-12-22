package com.chan.common.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author: chenye
 * @Date: 2020/2/29 16:15
 * @Blog:
 * @Description:
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class TopScorers {

    /**
     * 排名，球员，国籍，俱乐部，总进球，主场进球数，客场进球数，点球进球数
     */
    public int rank;
    public String player;
    public String country;
    public String club;
    public int total_score;
    public int total_score_home;
    public int total_score_visit;
    public int point_kick;

}
