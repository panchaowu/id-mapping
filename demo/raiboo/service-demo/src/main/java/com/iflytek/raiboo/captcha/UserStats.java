package com.iflytek.raiboo.captcha;

import com.google.gson.Gson;
import com.iflytek.raiboo.Dbutil;
import org.apache.commons.dbutils.handlers.ScalarHandler;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by jcao2014 on 2016/12/12.
 */
public class UserStats {
//    参与次数
//    select count(*) from raiboo_captcha_message;
//
//    领取奖品数量
//    select count(*) from raiboo_captcha_message where isvalid=0;
//
//    参与用户数
//    select count(distinct phonenumber) from raiboo_captcha_message;
//
//    领取奖品的用户数
//    select count(distinct phonenumber) from raiboo_captcha_message where isvalid=0;
    public Map<String,Integer> count() throws SQLException {
        Dbutil db = new Dbutil();
        String sql = "select count(*) from raiboo_captcha_message";
        int userPV = ((Long)db.run.query(sql, new ScalarHandler(1))).intValue();
        sql = "select count(*) from raiboo_captcha_message where isvalid=0";
        int giftPV = ((Long)db.run.query(sql, new ScalarHandler(1))).intValue();
        sql = "select count(distinct phonenumber) from raiboo_captcha_message";
        int userUV = ((Long)db.run.query(sql, new ScalarHandler(1))).intValue();
        sql = "select count(distinct phonenumber) from raiboo_captcha_message where isvalid=0";
        int giftUV = ((Long)db.run.query(sql, new ScalarHandler(1))).intValue();
        Map<String,Integer> map=new HashMap<String,Integer>();
        map.put("userPV", userPV);
        map.put("giftPV", giftPV);
        map.put("userUV", userUV);
        map.put("giftUV", giftUV);
        return map;
    };
    public static void main(String[] args) throws SQLException {
        Gson gson = new Gson();
        UserStats us = new UserStats();
        System.out.println(gson.toJson(us.count()));
    }

}
