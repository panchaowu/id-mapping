package com.iflytek.raiboo.promotion;

import com.iflytek.raiboo.Dbutil;
import jersey.repackaged.com.google.common.collect.Maps;
import org.apache.commons.dbutils.ResultSetHandler;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;


/**
 * Created by jcao2014 on 2016/12/2.
 */
public class Promotion {

    public Map<Long, Integer> readDB(String market_id) throws SQLException {

        Dbutil db = new Dbutil();
        Map<Long, Integer> market_brand_id = null;

        // get message_id, content, market_id and brand_id from raiboo_promotion_message_content

        // get market_id and brand_id from raiboo_probe
        ResultSetHandler<Map<Long, Integer>> h = new ResultSetHandler<Map<Long, Integer>>() {
            public Map<Long, Integer> handle(ResultSet rs) throws SQLException {
                Map<Long, Integer> result = Maps.newHashMap();
                while (rs.next()) {
                    result.put(rs.getLong(1), rs.getInt(2));
                }
                return result;
            }
        };
        try {
            market_brand_id = db.run.query("select market_id, brand_id from raiboo_probe where market_id=?", h, Long.valueOf(market_id).longValue());
        } catch (SQLException sqle) {
            sqle.printStackTrace();
        }
//        System.out.println("get brand_id" + market_brand_id.toString());
        return market_brand_id;
    }

    public void updateDB(String mac, String phonenumber, Map<Long, Integer> marketBrandId) throws SQLException, IOException, InterruptedException {

        Dbutil db = new Dbutil();
        int timestamp = (int) (System.currentTimeMillis() / 1000);
        String market_id = "0", brand_id = "0";
        for (Map.Entry<Long, Integer> entry : marketBrandId.entrySet()) {
            market_id = entry.getKey().toString();
            brand_id = entry.getValue().toString();
        }
        // insert into raiboo_promotion_message_data
        try {
            db.run.update("insert into raiboo_promotion_message_data (timestamp, message_id, market_id, brand_id, mac, phonenumber) values (?, ?, ?, ?, ?, ?)",
                    timestamp, "0", market_id, brand_id, mac, phonenumber);

        } catch (SQLException sqle) {
            sqle.printStackTrace();
        }
//        System.out.println("=======");

//        // update raiboo_analysis_realtime_message_hourly
//        String market_id = "0", brand_id = "0";
//        for (Map.Entry<Long, Integer> entry : marketBrandId.entrySet()) {
//            market_id = entry.getKey().toString();
//            brand_id = entry.getValue().toString();
//        }
//        SimpleDateFormat df = new SimpleDateFormat("mm");
////        java.sql.Timestamp ts = new java.sql.Timestamp(new java.util.Date().getTime());
////        System.out.println(ts.getTime());
////        System.out.println(Integer.parseInt(df.format(new Date())));
//        if (Integer.parseInt(df.format(new Date())) == 00) {
//            db.run.update(
//                    "insert into raiboo_analysis_realtime_message_hourly (message_id, market_id, brand_id, sess_times) values (?, ?, ?, ?)"
//                    , "0", market_id, brand_id, 1//, ts
//            );
//        } else {
////            System.out.println(market_id + "=======");
//            db.run.update(
//                    "update raiboo_analysis_realtime_message_hourly  a, (select max(id) id from raiboo_analysis_realtime_message_hourly) b set market_id=?, brand_id=?, sess_times = sess_times+1 where a.id=b.id"
//                    , market_id, brand_id
//            );
//        }
    }

}
